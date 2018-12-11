"""
Check lengths of RabbitMQ queues and exit with proper return code based on
thresholds defined in provided configuration file.
"""

import logging
import os
import sys
from contextlib import contextmanager

import yaml
from argh import arg, dispatch_command
from pyrabbit.api import Client
from pyrabbit.http import NetworkError, HTTPError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('check_rabbitmq_queues')


DEFAULT_CONFIG = '/usr/local/etc/check_rabbitmq_queues.yml'
DEFAULT_USERNAME = 'guest'
DEFAULT_PASSWORD = 'guest'
DEFAULT_VHOST = '/'
DEFAULT_HOSTNAME = 'localhost'
DEFAULT_PORT = 15672


class RabbitException(Exception):

    def __init__(self, errors, stats={}):
        self.errors = errors
        self.stats = stats


class RabbitWarning(RabbitException):
    error_code = 1
    prefix = 'WARNING - %s.'


class RabbitCritical(RabbitException):
    error_code = 2
    prefix = 'CRITICAL - %s.'


def get_config(config_path):
    """
    Try to load script config from yaml file, exit with code 3 if file does not
    exists.
    :param config_path: path to config file
    :return: config dict
    """
    if not os.path.exists(config_path):
        logger.error('Configuration file %s does not exist.' % config_path)
        sys.exit(3)
    return yaml.load(open(config_path))


def get_client(cfg):
    """
    Get RabbitMQ client based on provided configuration or default settings.
    :param cfg: config dict
    :return: RabbitMQ client object
    """
    username_from_env = os.getenv('CHECK_RABBITMQ_QUEUES_USERNAME')
    password_from_env = os.getenv('CHECK_RABBITMQ_QUEUES_PASSWORD')

    client = Client('%s:%s' % (cfg.get('host', DEFAULT_HOSTNAME),
                               cfg.get('port', DEFAULT_PORT)),
                    username_from_env or cfg.get('username', DEFAULT_USERNAME),
                    password_from_env or cfg.get('password', DEFAULT_PASSWORD))
    return client


@contextmanager
def supress_output():
    """
    Supress debug output
    """
    stdout = sys.stdout
    temp_stdout = open(os.devnull, 'w')
    sys.stdout = temp_stdout
    try:
        yield temp_stdout
    finally:
        sys.stdout = stdout
        temp_stdout.close()


def check_queue(queue, config):
    """
    Check length and policy of a single queue
    :param queue: Queue form rabbit
    :param config: Desired queue state
    :return: length of a queue and lists of warnings and errors
    """
    warnings = []
    errors = []
    length = queue['messages_ready']
    if config:
        if length > config['critical']:
            errors.append(length)
        elif length > config['warning']:
            warnings.append(length)

        policy = config.get('policy')
        queue_policy = queue.get('effective_policy_definition')
        if policy and policy != queue_policy:
            errors.append('Wrong queue policy')

    return length, warnings, errors


def check_lengths(queues, queue_conf, queue_prefix_conf):
    """
    Check queues length and policy
    :param queues: Queues from rabbit
    :param queue_conf: Queues to check
    :raises: RabbitException with list of faulty queues and reasons
    :return: Queues lengths
    """
    errors = {}
    warnings = {}
    stats = {}
    prefixes = sorted(queue_prefix_conf.keys(), key=len, reverse=True)
    for queue in queues:
        try:
            name = queue['name']
        except KeyError:
            pass
        else:
            config = None
            if name in queue_conf:
                config = queue_conf[name]
            else:
                prefix = next((p for p in prefixes if name.startswith(p)),
                              None)
                if prefix:
                    config = queue_prefix_conf[prefix]

            length, queue_warnings, queue_errors = check_queue(queue, config)
            if queue_errors:
                errors[name] = queue_errors
            elif queue_warnings:
                warnings[name] = queue_warnings
            stats[name] = length

    missing = list(filter(lambda q: q not in stats, queue_conf.keys()))
    for q in missing:
        errors[q] = ['Queue not found']

    if errors:
        raise RabbitCritical(errors, stats)
    elif warnings:
        raise RabbitWarning(warnings, stats)

    return stats


def format_status(errors):
    """
    Get formatted string with lengths of all queues from errors list.
    :param errors: list of queues with too many messages within
    :param stats: dict with lengths of all queues
    :return: formatted string
    """
    msg = ' '.join('%s(%s)' % (q, errors[q]) for q in errors)
    return msg


def get_queues(client, vhost):
    """
    Get all queues info from rabbitmq
    :param client: RabbitMQ client object
    :param vhost: RabbitMQ vhost name
    """
    try:
        with supress_output():
            return client.get_queues(vhost)
    except (NetworkError, HTTPError) as e:
        if isinstance(e, NetworkError):
            warning = 'Can not communicate with RabbitMQ.'
        elif e.status == 404:
            warning = 'Queue not found.'
        elif e.status == 401:
            warning = 'Unauthorized.'
        else:
            warning = 'Unhandled HTTP error, status: %s' % e.status
        raise RabbitCritical({'all': [warning]})


@arg('-c', '--config', help='Path to config')
def run(config=DEFAULT_CONFIG):
    """
    Check queues lengths basing on thresholds from provided config, exit from
    script with return code 2 when there were queues with number of messages
    greater than critical threshold, return code 1 when there where queues with
    number of messages greater than warning threshold or there was error during
    communicating with RabbitMQ and return code 0 when all queues have decent
    lengths. In all cases print message with status and in case of exceeding
    thresholds with affected queues names and lengths.
    :param config: path to config
    """
    cfg = get_config(config)

    vhost = cfg.get('vhost', DEFAULT_VHOST)
    queues_conf = cfg.get('queues', {})
    prefixes_conf = cfg.get('queue_prefixes', {})

    client = get_client(cfg)

    try:
        queues = get_queues(client, vhost)
        check_lengths(queues, queues_conf, prefixes_conf)
    except RabbitException as e:
        print(e.prefix % format_status(e.errors))
        sys.exit(e.error_code)
    else:
        print('OK - all lengths fine.')
        sys.exit(0)


def main():
    """
    Dispatch 'run' command and break script with return code 1 and proper
    message in case of any exception.
    """
    try:
        dispatch_command(run)
    except Exception as e:
        print('WARNING - unhandled Exception: %s' % str(e))
        if os.getenv('CHECK_QUEUES_DEBUG'):
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

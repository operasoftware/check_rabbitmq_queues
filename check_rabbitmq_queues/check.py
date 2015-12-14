"""
Check lengths of RabbitMQ queues and exit with proper return code based on
thresholds defined in provided configuration file.
"""

from collections import namedtuple
import logging
import os
import sys

from argh import arg, dispatch_command
import yaml

from pyrabbit.api import Client
from pyrabbit.http import NetworkError, HTTPError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('check_rabbitmq_queues')
Stats = namedtuple('Stats', ['lengths', 'errors'])


DEFAULT_CONFIG = '/usr/local/etc/check_rabbitmq_queues.yml'
DEFAULT_USERNAME = 'guest'
DEFAULT_PASSWORD = 'guest'
DEFAULT_VHOST = '/'
DEFAULT_HOSTNAME = 'localhost'
DEFAULT_PORT = 15672


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
    client = Client('%s:%s' % (cfg.get('host', DEFAULT_HOSTNAME),
                               cfg.get('port', DEFAULT_PORT)),
                    cfg.get('username', DEFAULT_USERNAME),
                    cfg.get('password', DEFAULT_PASSWORD))
    return client


def check_lengths(client, vhost, queues):
    """
    Check queues lengths.
    :param client: RabbitMQ client object
    :param vhost: RabbitMQ vhost name
    :param queues: queues to check
    :return: Stats(lengths=dict(queue_name: length),
                   errors=dict('critical': list queues with critical lengths,
                               'warning': list of queues with warning lengths))
    """

    stats = Stats(lengths={}, errors={'critical': [], 'warning': []})

    for queue, thresholds in queues.items():
        try:
            length = client.get_queue_depth(vhost, queue)
        except (NetworkError, HTTPError) as e:
            if isinstance(e, NetworkError):
                warning = 'Can not communicate with RabbitMQ.'
            elif e.status == 404:
                warning = 'Queue not found.'
            elif e.status == 401:
                warning = 'Unauthorized.'
            else:
                warning = 'Unhandled HTTP error, status: %s' % e.status

            stats.errors['warning'].append(queue)
            stats.lengths[queue] = warning

        else:
            length = int(length)

            if length > thresholds['critical']:
                stats.errors['critical'].append(queue)
            elif length > thresholds['warning']:
                stats.errors['warning'].append(queue)

            stats.lengths[queue] = length

    return stats


def format_status(errors, stats):
    """
    Get formatted string with lengths of all queues from errors list.
    :param errors: list of queues with too many messages within
    :param stats: dict with lengths of all queues
    :return: formatted string
    """
    msg = ' '.join('%s(%s)' % (q, stats[q]) for q in errors)
    return msg


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
    queues = cfg.get('queues', {})

    client = get_client(cfg)
    stats, errors = check_lengths(client, vhost, queues)

    if errors['critical']:
        print 'CRITICAL - %s.' % format_status(errors['critical'], stats)
        sys.exit(2)
    elif errors['warning']:
        print 'WARNING - %s.' % format_status(errors['warning'], stats)
        sys.exit(1)
    else:
        print 'OK - all lengths fine.'
        sys.exit(0)


def main():
    """
    Dispatch 'run' command and break script with return code 1 and proper
    message in case of any exception.
    """
    try:
        dispatch_command(run)
    except Exception as e:
        print 'WARNING - unhandled Exception: %s' % str(e)
        sys.exit(1)


if __name__ == "__main__":
    main()

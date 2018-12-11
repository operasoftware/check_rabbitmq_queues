from copy import copy
from unittest import TestCase

from mock import patch, Mock
from pyrabbit.http import NetworkError, HTTPError

from check_rabbitmq_queues.check import (
    check_lengths,
    get_queues,
    RabbitCritical,
    RabbitWarning,
    run,
)

MODULE = 'check_rabbitmq_queues.check'


class CheckLengthsTestCase(TestCase):
    thresholds = {'warning': 100, 'critical': 1000}
    normal = thresholds['warning'] - 1
    warning = thresholds['warning'] + 1
    critical = thresholds['critical'] + 1

    def test_ok(self):
        policy_mock = Mock()
        conf = copy(self.thresholds)
        conf['effective_policy_definition'] = policy_mock
        queue_conf = {'foo': conf}
        queue_prefix_conf = {}
        queues = [{'name': 'foo', 'messages_ready': self.normal,
                   'policy': policy_mock}]

        res = check_lengths(queues, queue_conf, queue_prefix_conf)
        self.assertEqual(res, {'foo': self.normal})

    def test_warning(self):
        queue_conf = {'foo': self.thresholds}
        queue_prefix_conf = {'local_': self.thresholds}
        queues = [
            {'name': 'foo', 'messages_ready': self.warning},
            {'name': 'local_bar', 'messages_ready': self.warning},
        ]

        with self.assertRaises(RabbitWarning) as excinfo:
            check_lengths(queues, queue_conf, queue_prefix_conf)

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'foo': self.warning,
                                     'local_bar': self.warning})
        self.assertEqual(exc.errors, {'foo': [self.warning],
                                      'local_bar': [self.warning]})

    def test_critical(self):
        queue_conf = {'foo': self.thresholds}
        queue_prefix_conf = {'local_': self.thresholds}
        queues = [
            {'name': 'foo', 'messages_ready': self.critical},
            {'name': 'local_bar', 'messages_ready': self.critical},
        ]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf, queue_prefix_conf)

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'foo': self.critical,
                                     'local_bar': self.critical})
        self.assertEqual(exc.errors, {'foo': [self.critical],
                                      'local_bar': [self.critical]})

    def test_desired_queue_not_in_rabbit(self):
        queue_conf = {'foo': self.thresholds, 'bar': self.thresholds}
        queue_prefix_conf = {'test_': self.thresholds}
        queues = [{'name': 'foo', 'messages_ready': self.warning}]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf, queue_prefix_conf)

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'foo': self.warning})
        self.assertEqual(exc.errors, {'bar': ['Queue not found']})

    def test_criticals_take_precedence_over_warnings(self):
        queue_conf = {'foo': self.thresholds, 'bar': self.thresholds}
        queue_prefix_conf = {}
        queues = [
            {'name': 'foo', 'messages_ready': self.warning},
            {'name': 'bar', 'messages_ready': self.critical},
        ]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf, queue_prefix_conf)

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'foo': self.warning,
                                     'bar': self.critical})
        self.assertEqual(exc.errors, {'bar': [self.critical]})

    def test_override_prefix_threshold(self):
        lower_thresholds = {
            'warning': self.thresholds['warning']/2,
            'critical': self.thresholds['critical']/2,
        }
        lower_warning = lower_thresholds['warning'] + 1
        self.assertTrue(lower_warning < self.thresholds['warning'])

        queue_conf = {'test_foo': lower_thresholds}
        queue_prefix_conf = {'test_': self.thresholds}
        queues = [{'name': 'test_foo', 'messages_ready': lower_warning}]

        with self.assertRaises(RabbitWarning) as excinfo:
            check_lengths(queues, queue_conf, queue_prefix_conf)

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'test_foo': lower_warning})
        self.assertEqual(exc.errors, {'test_foo': [lower_warning]})

    def test_policy_is_wrong(self):
        conf = copy(self.thresholds)
        conf['policy'] = {'max-length': 500}
        queue_conf = {'test_foo': conf}
        queues = [{'name': 'test_foo', 'messages_ready': self.normal,
                   'effective_policy_definition': {'max-length': 100}}]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf, {})

        exc = excinfo.exception
        self.assertEqual(exc.stats, {'test_foo': self.normal})
        self.assertEqual(exc.errors, {'test_foo': ['Wrong queue policy']})


class GetQueuesTestCase(TestCase):

    def setUp(self):
        self.client_mock = Mock()
        self.vhost_mock = Mock()

    def test_ok(self):
        res = get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(res, self.client_mock.get_queues.return_value)
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_network_error(self):
        self.client_mock.get_queues.side_effect = NetworkError()

        with self.assertRaises(RabbitCritical) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors,
                         {'all': ['Can not communicate with RabbitMQ.']})
        self.assertEqual(excinfo.exception.stats, {})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_404(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=404)

        with self.assertRaises(RabbitCritical) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, {'all':
                                                    ['Queue not found.']})
        self.assertEqual(excinfo.exception.stats, {})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_401(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=401)

        with self.assertRaises(RabbitCritical) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, {'all': ['Unauthorized.']})
        self.assertEqual(excinfo.exception.stats, {})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_unknown_error(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=500)

        with self.assertRaises(RabbitCritical) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors,
                         {'all': ['Unhandled HTTP error, status: 500']})
        self.assertEqual(excinfo.exception.stats, {})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)


@patch(MODULE + '.get_config', Mock())
@patch(MODULE + '.format_status', Mock())
@patch(MODULE + '.check_lengths', Mock())
@patch(MODULE + '.get_queues')
@patch(MODULE + '.sys.exit')
class RunTestCase(TestCase):

    def test_ok(self, exit_mock, get_queues_mock):
        run()
        exit_mock.assert_called_once_with(0)

    def test_warning(self, exit_mock, get_queues_mock):
        get_queues_mock.side_effect = RabbitWarning(['all'])

        run()
        exit_mock.assert_called_once_with(1)

    def test_critical(self, exit_mock, get_queues_mock):
        get_queues_mock.side_effect = RabbitCritical(['all'])

        run()
        exit_mock.assert_called_once_with(2)

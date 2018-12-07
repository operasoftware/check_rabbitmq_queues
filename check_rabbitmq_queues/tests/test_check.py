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
        queue_conf = {'foo': self.thresholds}
        queues = [{'name': 'foo', 'messages': self.normal}]

        res = check_lengths(queues, queue_conf)
        self.assertEqual(res, {'foo': self.normal})

    def test_warning(self):
        queue_conf = {'foo': self.thresholds}
        queues = [{'name': 'foo', 'messages': self.warning}]

        with self.assertRaises(RabbitWarning) as excinfo:
            check_lengths(queues, queue_conf)

        exc = excinfo.exception
        self.assertEqual(exc.errors, ['foo'])
        self.assertEqual(exc.stats, {'foo': self.warning})

    def test_critical(self):
        queue_conf = {'foo': self.thresholds}
        queues = [{'name': 'foo', 'messages': self.critical}]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf)

        exc = excinfo.exception
        self.assertEqual(exc.errors, ['foo'])
        self.assertEqual(exc.stats, {'foo': self.critical})

    def test_desired_queue_not_in_rabbit(self):
        queue_conf = {'foo': self.thresholds, 'bar': self.thresholds}
        queues = [{'name': 'foo', 'messages': self.warning}]

        with self.assertRaises(RabbitWarning) as excinfo:
            check_lengths(queues, queue_conf)

        exc = excinfo.exception
        self.assertEqual(exc.errors, ['foo', 'bar'])
        self.assertEqual(exc.stats, {'foo': self.warning,
                                     'bar': 'Queue not found'})

    def test_criticals_take_precedence_over_warnings(self):
        queue_conf = {'foo': self.thresholds, 'bar': self.thresholds}
        queues = [
            {'name': 'foo', 'messages': self.warning},
            {'name': 'bar', 'messages': self.critical},
        ]

        with self.assertRaises(RabbitCritical) as excinfo:
            check_lengths(queues, queue_conf)

        exc = excinfo.exception
        self.assertEqual(exc.errors, ['bar'])
        self.assertEqual(exc.stats, {'foo': self.warning,
                                     'bar': self.critical})


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

        with self.assertRaises(RabbitWarning) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, ['all'])
        self.assertEqual(excinfo.exception.stats,
                         {'all': 'Can not communicate with RabbitMQ.'})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_404(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=404)

        with self.assertRaises(RabbitWarning) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, ['all'])
        self.assertEqual(excinfo.exception.stats, {'all': 'Queue not found.'})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_401(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=401)

        with self.assertRaises(RabbitWarning) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, ['all'])
        self.assertEqual(excinfo.exception.stats, {'all': 'Unauthorized.'})
        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_unknown_error(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=500)

        with self.assertRaises(RabbitWarning) as excinfo:
            get_queues(self.client_mock, self.vhost_mock)

        self.assertEqual(excinfo.exception.errors, ['all'])
        self.assertEqual(excinfo.exception.stats,
                         {'all': 'Unhandled HTTP error, status: 500'})
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

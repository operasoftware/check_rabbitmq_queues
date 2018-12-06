from unittest import TestCase

from mock import patch, Mock
from pyrabbit.http import NetworkError, HTTPError

from check_rabbitmq_queues.check import (
    check_lengths,
    get_queues,
    RabbitWarning,
    run,
)

MODULE = 'check_rabbitmq_queues.check'


class CheckLengthsTestCase(TestCase):
    thresholds = {'warning': 100, 'critical': 1000}
    normal = thresholds['warning'] - 1

    def test_ok(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.return_value = self.normal

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': client_mock.get_queue_depth.return_value})
        self.assertEqual(errors, {'critical': [], 'warning': []})

    def test_warning(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.return_value =\
            self.thresholds['warning'] + 1

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': client_mock.get_queue_depth.return_value})
        self.assertEqual(errors, {'critical': [], 'warning': ['foo']})

    def test_critical(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.return_value =\
            self.thresholds['critical'] + 1

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': client_mock.get_queue_depth.return_value})
        self.assertEqual(errors, {'critical': ['foo'], 'warning': []})

    def test_network_error(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.side_effect = NetworkError()

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': 'Can not communicate with RabbitMQ.'})
        self.assertEqual(errors, {'critical': [], 'warning': ['foo']})

    def test_404(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.side_effect = HTTPError('', status=404)

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': 'Queue not found.'})
        self.assertEqual(errors, {'critical': [], 'warning': ['foo']})

    def test_401(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.side_effect = HTTPError('', status=401)

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': 'Unauthorized.'})
        self.assertEqual(errors, {'critical': [], 'warning': ['foo']})

    def test_unknown_error(self):
        queues = {'foo': self.thresholds}
        client_mock = Mock()
        client_mock.get_queue_depth.side_effect = HTTPError('', status=500)

        stats, errors = check_lengths(client_mock, Mock(), queues)

        self.assertEqual(
            stats, {'foo': 'Unhandled HTTP error, status: 500'})
        self.assertEqual(errors, {'critical': [], 'warning': ['foo']})


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

        with self.assertRaises(RabbitWarning):
            get_queues(self.client_mock, self.vhost_mock)

        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_404(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=404)

        with self.assertRaises(RabbitWarning):
            get_queues(self.client_mock, self.vhost_mock)

        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_401(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=401)

        with self.assertRaises(RabbitWarning):
            get_queues(self.client_mock, self.vhost_mock)

        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)

    def test_unknown_error(self):
        self.client_mock.get_queues.side_effect = HTTPError('', status=500)

        with self.assertRaises(RabbitWarning):
            get_queues(self.client_mock, self.vhost_mock)

        self.client_mock.get_queues.assert_called_once_with(self.vhost_mock)


@patch(MODULE + '.get_config', Mock())
@patch(MODULE + '.format_status', Mock())
@patch(MODULE + '.check_lengths')
@patch(MODULE + '.sys.exit')
class RunTestCase(TestCase):

    def test_ok(self, exit_mock, check_lengths_mock):
        check_lengths_mock.return_value = Mock(), \
                                          {'warning': [], 'critical': []}
        run()
        exit_mock.assert_called_once_with(0)

    def test_warning(self, exit_mock, check_lengths_mock):
        check_lengths_mock.return_value = Mock(), \
                                          {'warning': ['foo'], 'critical': []}
        run()
        exit_mock.assert_called_once_with(1)

    def test_critical(self, exit_mock, check_lengths_mock):
        check_lengths_mock.return_value = Mock(), \
                                          {'warning': [], 'critical': ['foo']}
        run()
        exit_mock.assert_called_once_with(2)

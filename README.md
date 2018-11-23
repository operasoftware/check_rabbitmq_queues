# check_rabbitmq_queues #
Nagios plugin written in python for checking if queue lengths do not exceed thresholds specified in config.

## Installation ##
```
pip install check-rabbitmq-queues
```

## Usage ##
```
check_rabbitmq_queues -c <path_to_config>
```

## Example config ##
```yaml
host: localhost
port: 15672
username: guest
password: guest
vhost: /
queues:
    queue1:
        critical: 0
        warning: 0
    queue2:
        critical: 0
        warning: 0
```

You can also provide username and password in `CHECK_RABBITMQ_QUEUES_USERNAME` and `CHECK_RABBITMQ_QUEUES_PASSWORD`
environmental variables instead if you wish.

## Tests ##
In project directory:
```
tox
```

#!/usr/bin/env python
from setuptools import setup


setup(name='check-rabbitmq-queues',
      version='1.5.1',
      description='Package for checking current length of RabbitMQ queues.',
      author='Opera Services Team',
      author_email='svc-code@opera.com',
      packages=['check_rabbitmq_queues'],
      install_requires=['pyrabbit2==1.0.7',
                        'argh==0.26.1',
                        'PyYAML==5.4'],
      entry_points={
          'console_scripts': [
              'check_rabbitmq_queues = check_rabbitmq_queues.check:main'
          ]
      })

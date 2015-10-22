#!/bin/sh

# phpamqplib:phpamqplib_password has full access to phpamqplib_testbed

rabbitmqctl add_vhost phpamqplib_testbed
rabbitmqctl add_user phpamqplib phpamqplib_password
rabbitmqctl set_permissions -p phpamqplib_testbed phpamqplib ".*" ".*" ".*"

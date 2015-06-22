#!/bin/sh

exec "$1" "${@:2}" <&- &
sleep 2
exec $(dirname $0)/enable_drivers_single_node.py

sleep 8

#!/bin/sh

exec "$1" "${@:2}" <&- &

sleep 10

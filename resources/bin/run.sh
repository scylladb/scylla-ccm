#!/bin/sh

i=0
until [ ${!i} == '--options-file' ];
do
   let i=$i+1
done
let i=$i+1
ip=`cat ${!i} | grep 'listen_address' | cut -f2 -d' '`
exec "$1" "${@:2}" <&- &
sleep 2
exec $(dirname $0)/enable_drivers_single_node.py $ip

sleep 8

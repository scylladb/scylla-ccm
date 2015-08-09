#!/bin/sh -x

# 1 - root path of node
# 3 - args

i=0
ip=`cat $1/conf/cassandra.yaml | grep 'listen_address' | cut -f2 -d' '`
jmx_port=`cat $1/node.conf | grep 'jmx_port' | cut -f2 -d"'"`
# FIXME workaround for log message
echo "Starting listening for CQL clients" >> $1/logs/system.log
echo "end facked message" >> $1
exec $1/bin/scylla --options-file $1/conf/cassandra.yaml "${@:2}" <&- 2>&1 | tee -a "$1/logs/system.log" &
sleep 2
# FIXME workaround for starting urchin-jmx - should use run script
pkill -f com.sun.management.jmxremote.port=$jmx_port || true
sleep 2
exec java -Dapiaddress=$ip -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$jmx_port -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar $1/bin/urchin-mbean-1.0.jar <&- 2>&1 | tee -a "$1/logs/system.log.jmx" &

sleep 6

#!/bin/sh -x

# 1 - root path of node
# 3 - args

i=0
ip=`cat $1/conf/cassandra.yaml | grep 'listen_address' | cut -f2 -d' '`
jmx_port=`cat $1/node.conf | grep 'jmx_port' | cut -f2 -d"'"`
echo "" >> $1/logs/system.log
export SCYLLA_HOME=$1
shift
exec $SCYLLA_HOME/bin/scylla --options-file $SCYLLA_HOME/conf/scylla.yaml "$@" <&- 2>&1 | tee -a "$SCYLLA_HOME/logs/system.log" &
pkill -9 -f com.sun.management.jmxremote.port=$jmx_port || true

sleep 6

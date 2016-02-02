#!/bin/bash -x

# 1 - root path of node
# 3 - args

i=0
ip=`cat $1/conf/cassandra.yaml | grep 'listen_address' | cut -f2 -d' '`
jmx_port=`cat $1/node.conf | grep 'jmx_port' | cut -f2 -d"'"`
export SCYLLA_HOME=$1
shift
pkill -9 -f com.sun.management.jmxremote.port=$jmx_port || true
sleep 2
count=0
tmp=/tmp/run.$$
echo "while kill -0 `cat $SCYLLA_HOME/cassandra.pid` && [[ (\$count < 10) ]] ; do let count+1 ; java -Dapiaddress=$ip -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$jmx_port -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar $SCYLLA_HOME/bin/scylla-jmx-1.0.jar <&- 2>&1 | tee -a $SCYLLA_HOME/logs/system.log.jmx; sleep 1; done ; rm $tmp" > $tmp
chmod 777 $tmp
exec $tmp &

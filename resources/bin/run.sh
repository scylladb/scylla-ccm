#!/bin/sh -x

# 1 - log file to write output
# 2 - executable
# 3 - args

i=0
until [ ${!i} == '--options-file' ];
do
   let i=$i+1
done
let i=$i+1
ip=`cat ${!i} | grep 'listen_address' | cut -f2 -d' '`
# FIXME workaround for log message
echo "Starting listening for CQL clients" >> $1
echo "end facked message" >> $1
exec "$2" "${@:3}" <&- 2>&1 | tee -a "$1" &
sleep 2
# FIXME workaround for starting urchin-jmx - should use run script
exec_dir=$(dirname $2)
exec java -Dapiaddress=$ip -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=7100 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar $exec_dir/urchin-mbean-1.0.jar <&- 2>$1.jmx &

sleep 8

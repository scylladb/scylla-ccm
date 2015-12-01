export PATH=`pwd`:$PATH
export PERL5LIB=`pwd`
export PYTHONPATH=`pwd`

echo "Examples:"
echo

echo "=== Create scylla cluster with single node"
echo "ccm create scylla-1 --urchin --vnodes -n 1 --install-dir=`pwd`/../scylla"
echo

echo "=== Create scylla cluster with 3 nodes"
echo "ccm create scylla-3 --urchin --vnodes -n 3 --install-dir=`pwd`/../scylla"
echo

echo "=== Create cassandra cluster with 3 nodes"
echo "ccm create cas-3  --vnodes -n 3 --install-dir=`pwd`/../scylla-tools-java"
echo

echo "=== List clusters"
echo "ccm list"
echo

echo "=== Start cluster"
echo "ccm start --no-wait"
echo

echo "=== Stop and remove"
echo "ccm remove"

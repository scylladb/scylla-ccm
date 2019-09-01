export PATH=`pwd`:$PATH
export PERL5LIB=`pwd`
export PYTHONPATH=`pwd`

echo "Examples:"
echo

echo "=== Create scylla cluster with single node"
echo "ccm create scylla-1 --scylla --vnodes -n 1 --install-dir=`pwd`/../scylla"
echo

echo "=== Create scylla cluster with 3 nodes"
echo "ccm create scylla-3 --scylla --vnodes -n 3 --install-dir=`pwd`/../scylla"
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


echo "=== Create scylla cluster with single node [relocatable]"
echo "ccm create scylla-reloc-1 -n 1 --scylla --version unstable/master:380 --scylla-core-package-uri=../scylla/build/dev/scylla-package.tar.gz"
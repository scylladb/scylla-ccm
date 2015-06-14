PATH=/home/shlomi/ccm:$PATH
export PATH
export PERL5LIB=`pwd`
export PYTHONPATH=`pwd`
echo "create single node"
echo "./ccm create urchin --urchin --vnodes -n 1 --install-dir=/home/shlomi/urchin2"
echo "create cluster"
echo "./ccm create urchin --urchin --vnodes -n 3 --install-dir=/home/shlomi/urchin2"
echo "./ccm list"
echo "./ccm start --no-wait"
echo "./ccm remove"

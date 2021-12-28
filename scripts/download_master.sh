# download enterprise latest relocatable from enterprise branch
#
# Usage:
#   ./scripts/download_enterprise.sh

export BRANCH=master

export LATEST_MASTER_JOB_ID=`aws --no-sign-request s3 ls downloads.scylladb.com/unstable/scylla/${BRANCH}/relocatable/ | grep '-' | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
AWS_BASE=s3://downloads.scylladb.com/unstable/scylla/${BRANCH}/relocatable/${LATEST_MASTER_JOB_ID}

NAME="unstable/master:$LATEST_MASTER_JOB_ID"

ccm create scylla-driver-temp -n 1 --scylla --version $NAME
ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=$NAME"

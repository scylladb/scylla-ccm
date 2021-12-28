# download enterprise latest relocatable from enterprise branch
#
# Usage:
#   ./scripts/download_enterprise.sh

BRANCH=enterprise
LATEST_ENTERPRISE_JOB_ID=`aws --no-sign-request s3 ls downloads.scylladb.com/unstable/scylla-enterprise/${BRANCH}/relocatable/ | grep '-' | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
AWS_BASE=s3://downloads.scylladb.com/enterprise/relocatable/unstable/enterprise/${LATEST_ENTERPRISE_JOB_ID}
AWS_BASE=s3://downloads.scylladb.com/unstable/scylla-enterprise/${BRANCH}/relocatable/${LATEST_ENTERPRISE_JOB_ID}

NAME="unstable/${BRANCH}:${LATEST_ENTERPRISE_JOB_ID}"
export SCYLLA_PRODUCT=scylla-enterprise
ccm create scylla-temp -n 1 --scylla --version $NAME
ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=$NAME"

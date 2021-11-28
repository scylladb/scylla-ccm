# download enterprise latest relocatable from enterprise branch
#
# Usage:
#   ./scripts/download_enterprise.sh

export BRANCH=master

export LATEST_MASTER_JOB_ID=`aws --no-sign-request s3 ls downloads.scylladb.com/unstable/scylla/${BRANCH}/relocatable/ | grep '-' | tr -s ' ' | cut -d ' ' -f 3 | tr -d '\/'  | sort -g | tail -n 1`
AWS_BASE=s3://downloads.scylladb.com/unstable/scylla/${BRANCH}/relocatable/${LATEST_MASTER_JOB_ID}

rm scylla-*.tar.gz

aws s3 --no-sign-request cp ${AWS_BASE}/scylla-x86_64-package.tar.gz .
aws s3 --no-sign-request cp ${AWS_BASE}/scylla-tools-package.tar.gz .
aws s3 --no-sign-request cp ${AWS_BASE}/scylla-jmx-package.tar.gz .

NAME=master_$LATEST_MASTER_JOB_ID
NAME=$(echo master_$LATEST_MASTER_JOB_ID | sed 's/:/_/g')

ccm create scylla-driver-temp -n 1 --scylla --version $NAME \
  --scylla-core-package-uri=./scylla-x86_64-package.tar.gz \
  --scylla-tools-java-package-uri=./scylla-tools-package.tar.gz \
  --scylla-jmx-package-uri=./scylla-jmx-package.tar.gz

ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=$NAME"

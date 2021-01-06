# download release candidates version from release direcorty
#
# Usage:
#   ./scripts/download_rc_version.sh 4.3.rc1
#   ./scripts/download_rc_version.sh 4.2 rc1

BRANCH=$1
VERSION=$2

AWS_BASE=s3://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-$BRANCH/

for f in `aws s3 ls ${AWS_BASE} | awk '{print $4}' | grep "${VERSION}" | grep 'scylla-tools\|scylla-package\|scylla-jmx'` ;
do
    aws s3 cp ${AWS_BASE}${f} .
    if [[ $f == *"scylla-package"* ]]; then
        export SCYLLA_CORE_PACKAGE=./$f
    fi
    if [[ $f == *"scylla-tools"* ]]; then
        export SCYLLA_TOOLS_JAVA_PACKAGE=./$f
    fi
    if [[ $f == *"scylla-jmx"* ]]; then
        export SCYLLA_JMX_PACKAGE=./$f
    fi
done

NAME=$(echo "${BRANCH}.${VERSION}" | sed 's/:/_/g')

ccm create temp_${NAME} -n 1 --scylla --version branch_${NAME}
ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=branch_${NAME}"

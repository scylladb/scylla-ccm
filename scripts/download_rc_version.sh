# download release candidates version from release direcorty
#
# Usage:
#   ./scripts/download_rc_version.sh 4.3.rc1
#   ./scripts/download_rc_version.sh 4.2 rc1

BRANCH=$1
VERSION=$2

NAME="$BRANCH.$VERSION"
ccm create temp_${NAME} -n 1 --scylla --version release:${NAME}
ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=release:${NAME}"

# download release candidates version from release direcorty
#
# Usage:
#   ./scripts/download_rc_version.sh 4.3
#   ./scripts/download_rc_version.sh 4.2

BRANCH=$1

NAME="$BRANCH"
ccm create temp_${NAME} -n 1 --scylla --version release:${NAME}
ccm remove

echo "now it can be used in dtest as:"
echo "export SCYLLA_VERSION=release:${NAME}"

# shellcheck shell=bash
# Resolve Flink locations from the repo / environment.
# Source this file; never copy a host-absolute path into a script default.
#
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   # repo-root scripts:  source "$SCRIPT_DIR/scripts/flink-env.sh"
#   # scripts/ callers:   source "$SCRIPT_DIR/flink-env.sh"
#
# Exports: REPO_ROOT, FLINK_VERSION, FLINK_HOME, FLINK_CONF_DIR,
#          FLINK_STATE_DIR, FLINK_COMMON_JAR, FLINK_BIN

_flink_env_this="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$_flink_env_this/../pyproject.toml" ]; then
  REPO_ROOT="$(cd "$_flink_env_this/.." && pwd)"
elif [ -f "$_flink_env_this/pyproject.toml" ]; then
  REPO_ROOT="$_flink_env_this"
else
  REPO_ROOT="${DEVENV_ROOT:-${PWD}}"
fi
unset _flink_env_this

FLINK_VERSION="${FLINK_VERSION:-1.20.1}"
if [ -z "${FLINK_HOME:-}" ]; then
  FLINK_HOME="$REPO_ROOT/thirdparty/flink/flink-dist/target/flink-${FLINK_VERSION}-bin/flink-${FLINK_VERSION}"
fi

if [ -z "${FLINK_STATE_DIR:-}" ]; then
  FLINK_STATE_DIR="${DEVENV_STATE:-$REPO_ROOT/.devenv/state}/flink"
fi

if [ -z "${FLINK_CONF_DIR:-}" ]; then
  if [ -n "${DEVENV_STATE:-}" ] && [ -d "${DEVENV_STATE}/flink/conf" ]; then
    FLINK_CONF_DIR="${DEVENV_STATE}/flink/conf"
  else
    FLINK_CONF_DIR="$FLINK_HOME/conf"
  fi
fi

if [ -z "${FLINK_COMMON_JAR:-}" ]; then
  _jar_dir="$REPO_ROOT/flink-cyber/flink-common/target"
  if [ -f "$_jar_dir/flink-common-2.4.0.jar" ]; then
    FLINK_COMMON_JAR="$_jar_dir/flink-common-2.4.0.jar"
  else
    FLINK_COMMON_JAR="$(ls -1 "$_jar_dir"/flink-common-*.jar 2>/dev/null | grep -v -e sources.jar -e javadoc.jar | head -1 || true)"
  fi
  unset _jar_dir
fi

FLINK_BIN="${FLINK_HOME}/bin/flink"

export REPO_ROOT FLINK_VERSION FLINK_HOME FLINK_CONF_DIR FLINK_STATE_DIR FLINK_COMMON_JAR FLINK_BIN

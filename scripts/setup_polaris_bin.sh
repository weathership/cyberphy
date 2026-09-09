#!/usr/bin/env bash
# Setup Polaris bin directory wrapper scripts
# These scripts wrap the Quarkus jars for devenv compatibility
#
# Usage: ./scripts/setup_polaris_bin.sh

set -e

POLARIS_HOME="${1:-thirdparty/polaris/polaris-bin-1.3.0-incubating}"

if [ ! -d "$POLARIS_HOME" ]; then
    echo "Error: Polaris home directory not found: $POLARIS_HOME"
    echo "Please ensure the Polaris binary distribution is extracted."
    exit 1
fi

BIN_DIR="$POLARIS_HOME/bin"
CONF_DIR="$POLARIS_HOME/conf"

# Create bin directory
mkdir -p "$BIN_DIR"

# Create admin wrapper script
cat > "$BIN_DIR/admin" << 'SCRIPT'
#!/usr/bin/env bash
# Polaris Admin CLI wrapper
# Usage: ./bin/admin <command> [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARIS_HOME="$(dirname "$SCRIPT_DIR")"

exec java -jar "$POLARIS_HOME/admin/quarkus-run.jar" "$@"
SCRIPT

# Create server wrapper script
cat > "$BIN_DIR/server" << 'SCRIPT'
#!/usr/bin/env bash
# Polaris Server wrapper
# Usage: ./bin/server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARIS_HOME="$(dirname "$SCRIPT_DIR")"

exec java -jar "$POLARIS_HOME/server/quarkus-run.jar" "$@"
SCRIPT

# Make scripts executable
chmod +x "$BIN_DIR/admin" "$BIN_DIR/server"

# Create conf directory with application.properties
mkdir -p "$CONF_DIR"

if [ ! -f "$CONF_DIR/application.properties" ]; then
    cat > "$CONF_DIR/application.properties" << 'PROPS'
# Polaris Server Configuration
# HTTP ports
quarkus.http.port=8181
quarkus.management.port=8182

# S3 / RustFS configuration for file IO (endpoint + keys via process env)
polaris.io.impl=org.apache.polaris.service.storage.s3.S3StorageIntegration
PROPS
fi

echo "Polaris bin scripts created in: $BIN_DIR"
echo "  - $BIN_DIR/admin"
echo "  - $BIN_DIR/server"
echo "  - $CONF_DIR/application.properties"

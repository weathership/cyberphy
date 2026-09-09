{ pkgs, lib, config, inputs, ... }:

let
  # ---------------------------------------------------------------------------
  # RustFS (local S3) — same pattern as ~/local/src/wxs/vigil
  # Overlay builds rustfs from github:rustfs/rustfs (not nixpkgs minio, insecure).
  # Service module is vendored at modules/rustfs.nix (pinned devenv modules lack it).
  # ---------------------------------------------------------------------------

  # mc wrapper: configures a "local" alias from RUSTFS_* env (port-aware).
  # When services.rustfs.bind is 0.0.0.0, RUSTFS_ADDRESS is "0.0.0.0:PORT" —
  # correct for the server, but clients should dial 127.0.0.1 (same as vigil).
  mc = pkgs.writeShellScriptBin "mc" ''
    set -euo pipefail
    CLIENT_DIR="''${RUSTFS_CLIENT_CONFIG_DIR:-$DEVENV_STATE/rustfs/mc}"
    mkdir -p "$CLIENT_DIR"
    ADDRESS="''${RUSTFS_ADDRESS:-127.0.0.1:9010}"
    # Rewrite wildcard bind → loopback for the S3 client URL
    case "$ADDRESS" in
      0.0.0.0:*) ADDRESS="127.0.0.1:''${ADDRESS#0.0.0.0:}" ;;
      [::]:*)    ADDRESS="[::1]:''${ADDRESS#\[::\]:}" ;;
      *:*)       ;;
    esac
    ACCESS="''${RUSTFS_ACCESS_KEY:-admin}"
    SECRET="''${RUSTFS_SECRET_KEY:-admin}"
    cat > "$CLIENT_DIR/config.json" <<EOF
    {
      "version": "10",
      "aliases": {
        "local": {
          "url": "http://''${ADDRESS}",
          "accessKey": "''${ACCESS}",
          "secretKey": "''${SECRET}",
          "api": "S3v4",
          "path": "auto"
        }
      }
    }
    EOF
    chmod 600 "$CLIENT_DIR/config.json" 2>/dev/null || true
    exec ${pkgs.minio-client}/bin/mc --config-dir "$CLIENT_DIR" "$@"
  '';

  # RUSTFS_DATA_DIR from dotenv/.env via getEnv (not config.env — avoids
  # infinite recursion when extraEnvironment merges into config.env).
  # Default: /raid/build/cyberphy/data/ (RAID, same pattern as vigil → /raid/build/vigil/data/).
  rustfsDataDir =
    let v = builtins.getEnv "RUSTFS_DATA_DIR";
    in if v != "" then v else "/raid/build/cyberphy/data/";

  # Local RustFS root credentials (MinIO is gone — simple lab defaults).
  localS3AccessKey = "admin";
  localS3SecretKey = "admin";
  # Project-standard ports (not rustfs defaults 9000/9001) — k8s + zarf assume 9010.
  localS3ApiPort = 9010;
  localS3ConsolePort = 9011;

  rustfsBuckets = [ "cyberphy" "cyberphy-hx" ];
in
{
  dotenv.enable = true;

  # Credentials — MINIO_* retained for Python paths that prefer MINIO_* when
  # S3_ENDPOINT is set (local object store vs real AWS profile).
  env.MINIO_ACCESS_KEY = localS3AccessKey;
  env.MINIO_SECRET_KEY = localS3SecretKey;
  env.S3_ENDPOINT = "http://localhost:${toString localS3ApiPort}";
  env.RUSTFS_ACCESS_KEY = localS3AccessKey;
  env.RUSTFS_SECRET_KEY = localS3SecretKey;
  env.RUSTFS_CLIENT_CONFIG_DIR = config.env.DEVENV_STATE + "/rustfs/mc";
  # Shell-visible data root (override anytime with RUSTFS_DATA_DIR in the environment).
  env.RUSTFS_DATA_DIR = rustfsDataDir;

  # Polaris catalog defaults (polaris-init / setup_polaris_catalog.sh)
  env.POLARIS_CATALOG_NAME = "cyberphy";
  env.POLARIS_WAREHOUSE = "s3://cyberphy/iceberg/warehouse";
  env.S3_BUCKET = "cyberphy";

  # Flink home - built from source in thirdparty/flink
  # FLINK_CONF_DIR is the runtime overlay (python.executable, etc.). Keep it out of
  # the Maven target/ dist so that tree stays relocatable across checkouts.
  env.FLINK_HOME = "${config.devenv.root}/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1";
  env.FLINK_CONF_DIR = "${config.devenv.root}/.devenv/state/flink/conf";
  env.KUBECONFIG = "${config.devenv.root}/.devenv/state/kubeconfig";

  # AWS S3 bucket for OTEL data - populated by `devenv tasks run aws:env` from tofu output
  # These are picked up by datagen and panel-viz ansible roles
  # Default to empty string so dotenv can override from .env file
  env.OTEL_S3_BUCKET = "";  # Set by aws:env task or .env file

  # RustFS via our overlay (official release binary WITH console UI).
  # Do NOT use inputs.rustfs.packages.*.default alone — that flake build skips
  # rustfs/static, so /rustfs/console/ 404s (no embedded frontend).
  # See nix/rustfs.nix and https://github.com/rustfs/rustfs/issues/4919
  overlays = [
    (final: prev: {
      rustfs = import ./nix/rustfs.nix {
        pkgs = prev;
        system = prev.stdenv.hostPlatform.system;
      };
    })
  ];

  # https://devenv.sh/packages/
  packages = with pkgs; [
    awscli2
    #claude-code
    cloudflared
    conftest
    d2
    dbmate
    flatbuffers
    flink
    git
    gh
    graphviz
    grpcurl
    imagemagick
    jq
    just
    k3d
    kubectl
    kubernetes-helm
    mc  # wrapped "local" alias using RUSTFS_* env vars at runtime
    mdbook
    mdbook-d2
    mdbook-katex
    mdbook-mermaid
    opentofu
    podman
    protobuf
    presenterm
    tilt
    zarf  # Air-gap packaging for K8s deployments
    zlib  # Required for numpy C extensions
  ];

  # Single-node RustFS — full replacement for services.minio.
  # See https://docs.rustfs.com/installation/linux/single-node-single-disk.html
  #
  # Port handling (devenv processes):
  # - services.rustfs allocates ports.api / ports.console from the bases below
  # - Prefer runtime RUSTFS_ADDRESS / RUSTFS_PORT over hardcoding in scripts
  # - bind 0.0.0.0 so RKE2/k3d pods reach host S3 at <node-ip>:9010
  services.rustfs = {
    enable = true;
    package = pkgs.rustfs; # from rustfs overlay input
    bind = "0.0.0.0";
    port = localS3ApiPort;
    consolePort = localS3ConsolePort;
    accessKey = localS3AccessKey;
    secretKey = localS3SecretKey;
    extraEnvironment = {
      RUSTFS_DATA_DIR = rustfsDataDir;
    };
  };

  # Pre-create bucket dirs under RUSTFS_DATA_DIR (on top of devenv:rustfs:setup).
  tasks."devenv:rustfs:buckets" = {
    exec = lib.concatStringsSep "\n" (
      map (b: ''mkdir -p "${rustfsDataDir}/${b}"'') rustfsBuckets
    );
    before = [ "devenv:processes:rustfs" ];
  };

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    extensions = ext: [
      ext.pg_cron  # Scheduled tasks
      ext.age      # Apache AGE - Graph database extension for lineage
    ];
    initialDatabases = [
      { name = "cybersec"; }
      { name = "metaflow"; }
      { name = "iceberg"; }
    ];
    port = 5438;
    listen_addresses = "*";  # Enable TCP from K8s pods and local clients
    settings = {
      shared_preload_libraries = "pg_cron,age";
      "cron.database_name" = "cybersec";
    };
    initialScript = ''
      -- Create cybersec user with login privileges
      CREATE USER cybersec WITH PASSWORD 'cybersec' LOGIN;
      
      -- Grant privileges on cybersec database
      GRANT ALL PRIVILEGES ON DATABASE cybersec TO cybersec;
      GRANT ALL PRIVILEGES ON DATABASE iceberg TO cybersec;
      
      -- Connect to iceberg database to create Polaris schema
      \c iceberg
      CREATE SCHEMA IF NOT EXISTS polaris_schema;
      GRANT ALL PRIVILEGES ON SCHEMA polaris_schema TO cybersec;
      ALTER SCHEMA polaris_schema OWNER TO cybersec;
      
      -- Create extensions
      \c cybersec
      CREATE EXTENSION IF NOT EXISTS pg_cron;
      -- AGE extension is created per-database in migrations
    '';
  };
  
  languages.python = {
    enable = true;
    package = pkgs.python312;
    uv.enable = true;
    uv.sync.enable = true;
    # PyFlink is a core dependency (not an extra).
    # Shell syncs notebook/Dask stack so `uv run` / Jupyter can use Holoviews without
    # manual --extra. k8s (dask) is compatible with flink after thirdparty/flink patch.
    # Omit allExtras so engine/benchmark stay opt-in (heavier / specialized).
    uv.sync.allExtras = false;
    uv.sync.extras = [ "dev" "observability" "k8s" ];
    venv.enable = true;
  };

  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk21;  # NiFi 2.0 requires Java 21+
    maven.enable = true;
  };

  languages.javascript = {
    enable = true;
    directory = "local-ui";
    npm = {
      enable = true;
      install.enable = true;
    };
  };

  # Shell initialization - runs on 'direnv allow' / entering the devenv shell
  # NOTE: Heavy operations (git submodule, Flink build) are handled by flink-bootstrap process
  # to avoid blocking shell startup. For first-time setup, run: devenv tasks run restart:clean
  enterShell = ''
    # Short alias for OpenTofu CLI
    alias tf="tofu"

    # Fix PyFlink editable install: remove conflicting pyflink directory from site-packages
    # The apache-flink-libraries package installs a pyflink/ dir with bin/lib/opt that shadows
    # the editable install from thirdparty/flink-python. This causes pyflink.__file__ = None.
    PYFLINK_SITEPACKAGES="$DEVENV_STATE/venv/lib/python3.12/site-packages/pyflink"
    if [ -d "$PYFLINK_SITEPACKAGES" ] && [ -f "$PYFLINK_SITEPACKAGES/README.txt" ]; then
      rm -rf "$PYFLINK_SITEPACKAGES"
    fi

    # macOS: prefer Podman machine connection for k3d/docker clients
    if [ "$(uname -s)" = "Darwin" ] && command -v podman >/dev/null 2>&1; then
      if [ -z "''${DOCKER_HOST:-}" ]; then
        if PODMAN_CONNS=$(podman system connection list --format json 2>/dev/null); then
          DEFAULT_URI=$(echo "$PODMAN_CONNS" | jq -r 'map(select(.Default==true)) | .[0].URI // empty')
          if [ -z "$DEFAULT_URI" ]; then
            DEFAULT_URI=$(echo "$PODMAN_CONNS" | jq -r 'map(select(.ReadWrite==true)) | .[0].URI // empty')
          fi
          if [ -n "$DEFAULT_URI" ]; then
            export DOCKER_HOST="$DEFAULT_URI"
            export K3D_HIDE_WARNING_ROOTLESS=1
            echo "Using Podman connection $DOCKER_HOST"
          fi
        fi
      fi
    fi

    # Do not auto-init Java/NiFi/Polaris submodules on shell enter — those trees
    # are multiple GB. PyFlink comes from vendored thirdparty/flink-python.
    # Operator opt-in: CYBERPHY_INIT_SUBMODULES=1 direnv reload
    if [ "''${CYBERPHY_INIT_SUBMODULES:-}" = "1" ] && [ -f "cybersec/bootstrap/submodules.py" ]; then
      uv run python -c "
from cybersec.bootstrap.submodules import prepare_all_submodules, is_submodule_initialized
import sys

needs_init = []
for name in ['flink', 'polaris', 'iceberg']:
    if not is_submodule_initialized(name):
        needs_init.append(name)

if needs_init:
    print(f'Initializing submodules: {needs_init}')
    results = prepare_all_submodules()
    for name, (ok, msg) in results.items():
        if not ok:
            print(f'  Warning: {msg}', file=sys.stderr)
" 2>/dev/null || true
    fi

    # Create Polaris bin wrapper scripts if needed
    POLARIS_HOME="$PWD/thirdparty/polaris/polaris-bin-1.3.0-incubating"
    if [ -d "$POLARIS_HOME" ] && [ ! -x "$POLARIS_HOME/bin/admin" ]; then
      echo "Creating Polaris bin wrapper scripts..."
      "$PWD/scripts/setup_polaris_bin.sh" "$POLARIS_HOME" 2>/dev/null || true
    fi

    # First-time setup hint - check for missing builds, not just submodules
    NEEDS_BOOTSTRAP=false
    if [ ! -f "thirdparty/flink/pom.xml" ]; then
      NEEDS_BOOTSTRAP=true
    elif [ ! -d "thirdparty/flink/flink-dist/target/flink-1.20.1-bin" ]; then
      NEEDS_BOOTSTRAP=true
    elif [ ! -d "$POLARIS_HOME/server" ]; then
      NEEDS_BOOTSTRAP=true
    fi

    if [ "$NEEDS_BOOTSTRAP" = "true" ]; then
      echo ""
      echo "First-time setup or missing builds detected."
      echo "Run: devenv tasks run restart:clean"
      echo "This initializes submodules and builds Flink + Polaris (~15 min on first run)"
      echo ""
    fi

    # Prefer a *working* kubeconfig (RKE2 first) over broken defaults
    if [ -f "$PWD/scripts/lab_env.sh" ]; then
      # shellcheck source=/dev/null
      source "$PWD/scripts/lab_env.sh"
      if resolve_kubeconfig; then
        export KUBECONFIG
      fi
      detect_k8s_target >/dev/null
      ensure_local_s3_env
    elif [ -z "''${CYBERSEC_K8S_TARGET:-}" ]; then
      DETECTED_K8S_TARGET="none"
      if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
        if grep -qE "rancher|rke2" "$KUBECONFIG" 2>/dev/null; then
          DETECTED_K8S_TARGET="rke2"
        elif grep -qE "k3d|k3s" "$KUBECONFIG" 2>/dev/null; then
          DETECTED_K8S_TARGET="k3d"
        fi
      fi
      export CYBERSEC_K8S_TARGET="$DETECTED_K8S_TARGET"
      export CYBERPHY_K8S_TARGET="$DETECTED_K8S_TARGET"
    fi
  '';
  
  languages.typescript = {
    enable=true;
  }; 

  tasks = {
    "docs:build".exec = "mdbook build docs/current";
    "docs:open".exec = "mdbook build docs/current --open";

    # Lab + RKE2 + Zarf NodePort status (shared resolver: scripts/lab_env.sh)
    "lab:status".exec = ''
      source scripts/lab_env.sh
      resolve_kubeconfig || true
      lab_status_report
    '';

    # Clean rebuild of Flink lib/ (Iceberg connectors + Hadoop client)
    # Use after submodule updates, version changes, or classpath issues
    "flink:rebuild-lib".exec = ''
      FLINK_DIST="$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1"

      if [ ! -d "$FLINK_DIST/lib" ]; then
        echo "ERROR: Flink not built yet. Run: devenv tasks run restart:clean"
        exit 1
      fi

      echo "=== Cleaning stale Hadoop/Iceberg JARs from Flink lib ==="
      rm -f "$FLINK_DIST/lib/"hadoop-*.jar
      rm -f "$FLINK_DIST/lib/"woodstox-core-*.jar
      rm -f "$FLINK_DIST/lib/"stax2-api-*.jar
      rm -f "$FLINK_DIST/lib/"iceberg-*.jar
      rm -f "$FLINK_DIST/lib/"aws-java-sdk-bundle-*.jar

      echo "=== Installing Iceberg connectors ==="
      if [ -f "thirdparty/iceberg/gradlew" ]; then
        cd thirdparty/iceberg
        ./gradlew -PflinkVersions=1.20 \
          :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar \
          :iceberg-aws-bundle:shadowJar \
          -x test -x integrationTest -x generateGitProperties \
          --no-daemon 2>&1 | grep -E "(BUILD|WARN|ERROR)" || true

        for jar in flink/v1.20/flink-runtime/build/libs/iceberg-flink-runtime-1.20-*.jar; do
          if [ -f "$jar" ] && [[ "$jar" != *"-sources.jar" ]] && [[ "$jar" != *"-javadoc.jar" ]]; then
            cp "$jar" "$FLINK_DIST/lib/"
            echo "Installed: $(basename $jar)"
            break
          fi
        done
        for jar in aws-bundle/build/libs/iceberg-aws-bundle-*.jar; do
          if [ -f "$jar" ] && [[ "$jar" != *"-sources.jar" ]] && [[ "$jar" != *"-javadoc.jar" ]]; then
            cp "$jar" "$FLINK_DIST/lib/"
            echo "Installed: $(basename $jar)"
            break
          fi
        done
        cd ../..
      else
        echo "ERROR: Iceberg submodule not initialized. Run: git submodule update --init thirdparty/iceberg"
        exit 1
      fi

      echo "=== Installing Hadoop client JARs ==="
      thirdparty/iceberg/gradlew -p thirdparty/hadoop-client \
        copyJars -PoutputDir="$FLINK_DIST/lib" \
        --no-daemon || exit 1

      echo "=== Verifying Flink lib ==="
      ls -1 "$FLINK_DIST/lib/"

      MISSING=0
      for class_check in \
        "hadoop-client-api:org/apache/hadoop/conf/Configuration.class" \
        "hadoop-client-runtime:org/apache/hadoop/shaded/org/apache/commons/configuration2/Configuration.class" \
        "iceberg-flink-runtime:org/apache/iceberg/flink/FlinkCatalogFactory.class"; do
        jar_prefix="''${class_check%%:*}"
        class="''${class_check##*:}"
        jar_file=$(ls "$FLINK_DIST/lib/$jar_prefix"*.jar 2>/dev/null | head -1)
        if [ -z "$jar_file" ]; then
          echo "FAIL: No $jar_prefix JAR found"
          MISSING=1
        elif ! jar tf "$jar_file" | grep -q "$class"; then
          echo "FAIL: $class not found in $(basename $jar_file)"
          MISSING=1
        else
          echo "OK: $class in $(basename $jar_file)"
        fi
      done

      if [ "$MISSING" = "1" ]; then
        echo "ERROR: Critical classes missing. Flink will not start."
        exit 1
      fi
      echo "=== Flink lib rebuild complete ==="
    '';

    # Policy validation using conftest
    "policy:check".exec = ''
      echo "Running policy validation..."

      # Generate environment config
      uv run python -c "
import asyncio
from cybersec.health.environment import write_environment_config
asyncio.run(write_environment_config())
print('Environment config written to build/environment.json')
"

      echo ""
      echo "Running conftest policies..."
      conftest test build/environment.json --policy policy/environment/ --all-namespaces || {
        echo ""
        echo "Policy violations detected. Fix issues above and re-run."
        exit 1
      }
      echo ""
      echo "All policy checks passed"
    '';

    "policy:generate".exec = ''
      echo "Generating environment config..."
      uv run python -c "
import asyncio
from cybersec.health.environment import write_environment_config
asyncio.run(write_environment_config())
print('Environment config written to build/environment.json')
"
      echo ""
      echo "Config written. Run validation with:"
      echo "  conftest test build/environment.json --policy policy/environment/"
    '';

    # Manual Polaris catalog initialization (normally runs automatically via polaris-init process)
    # Use this if automatic initialization failed or you need to re-initialize
    "polaris:init".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      
      log_info "Manually initializing Polaris catalog (cyberphy + RustFS)..."

      export POLARIS_CATALOG_NAME="''${POLARIS_CATALOG_NAME:-cyberphy}"
      export S3_ENDPOINT="''${S3_ENDPOINT:-http://localhost:9010}"
      export S3_BUCKET="''${S3_BUCKET:-cyberphy}"
      export S3_ACCESS_KEY="''${RUSTFS_ACCESS_KEY:-''${MINIO_ACCESS_KEY:-''${AWS_ACCESS_KEY_ID:-admin}}}"
      export S3_SECRET_KEY="''${RUSTFS_SECRET_KEY:-''${MINIO_SECRET_KEY:-''${AWS_SECRET_ACCESS_KEY:-admin}}}"
      export POLARIS_WAREHOUSE="''${POLARIS_WAREHOUSE:-s3://''${S3_BUCKET}/iceberg/warehouse}"
      
      # Check if Polaris is running
      if ! wait_for_polaris 1 0; then
        log_error "Polaris is not running. Start it with: devenv up"
        exit 1
      fi

      # RustFS must be up for warehouse base location
      if ! curl -sf --max-time 3 "$S3_ENDPOINT/health" >/dev/null 2>&1; then
        log_error "RustFS/S3 not reachable at $S3_ENDPOINT (start devenv / rustfs process)"
        exit 1
      fi
      
      # Trigger catalog initialization with retry
      if trigger_catalog_init 3 ./setup_polaris_catalog.sh; then
        log_success "Catalog initialized and verified successfully ($POLARIS_CATALOG_NAME)"
      else
        log_error "Catalog initialization failed after retries"
        exit 1
      fi
    '';
    
    # Verify complete Polaris bootstrap status
    "polaris:bootstrap-verify".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      verify_all
    '';
    
    "polaris:check".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      
      echo "Checking Polaris catalog status..."
      
      # Check if Polaris is running
      if ! wait_for_polaris 1 0; then
        echo "Polaris is not running. Start it with: devenv up"
        exit 1
      fi
      
      # Check if catalog exists (cyberphy + RustFS warehouse)
      if verify_catalog "cyberphy"; then
        echo "Polaris is properly configured (catalog=cyberphy, S3=RustFS)"
      else
        echo "Catalog 'cyberphy' not found"
        echo "   This should have been created automatically by the polaris-init process"
        echo "   To initialize manually, run: devenv tasks run polaris:init"
        exit 1
      fi
    '';
    
    # ============================================================================
    # AWS Deployment Tasks
    # ============================================================================
    # These tasks manage the AWS infrastructure and Kubernetes deployments.
    # Prerequisites: AWS credentials configured, SSH key at ~/.ssh/cybersec-dask.pem

    "aws:provision".exec = ''
      echo "🚀 Provisioning AWS infrastructure with OpenTofu..."
      echo ""
      echo "💡 Monitor progress in another terminal:"
      echo "   ./scripts/monitor_aws.sh --watch"
      echo ""
      cd infra/aws/tofu

      PROFILE="''${AWS_PROFILE:-default}"
      REGION="''${AWS_REGION:-$(aws configure get region --profile "$PROFILE" 2>/dev/null || echo "us-east-1")}"

      PREFIX=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_prefix; settings = SettingsManager(); config = settings.load(); print(get_developer_prefix(config))")
      EMAIL=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_email; settings = SettingsManager(); config = settings.load(); print(get_developer_email(config))")
      ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
      KEY_NAME="cybersec-dask-$PREFIX"

      export TF_VAR_developer_prefix="$PREFIX"
      export TF_VAR_developer_email="$EMAIL"
      export TF_VAR_ssh_key_name="$KEY_NAME"
      export TF_VAR_aws_region="$REGION"

      # Pin the tofu provider to the operator's resolved AWS profile so the
      # deployment lands in the same account that aws sts get-caller-identity
      # just confirmed above. Without this pin the provider falls back to the
      # SDK default chain, which may resolve to a different account than the
      # operator intended (see provider.tf comment).
      export TF_VAR_aws_profile="$PROFILE"

      # Ingress provider: cloudflare (default) or ngrok
      # Override with INGRESS_PROVIDER env var if needed
      export TF_VAR_ingress_provider="''${INGRESS_PROVIDER:-cloudflare}"

      # Cloudflare configuration (required when ingress_provider=cloudflare)
      export TF_VAR_cloudflare_account_id="''${CLOUDFLARE_ACCOUNT_ID:-}"
      export TF_VAR_cloudflare_zone_id="''${CLOUDFLARE_ZONE_ID:-}"

      # Dynamically detect available AZs (some regions like us-west-1 only have 2)
      AVAILABLE_AZS=$(aws ec2 describe-availability-zones --region "$REGION" --query 'AvailabilityZones[?State==`available`].ZoneName' --output json 2>/dev/null || echo '[]')
      if [ "$AVAILABLE_AZS" = "[]" ]; then
        # Fallback to common pattern if API call fails
        AVAILABLE_AZS="[\"''${REGION}a\",\"''${REGION}b\",\"''${REGION}c\"]"
      fi
      export TF_VAR_availability_zones="$AVAILABLE_AZS"

      echo "Developer: $EMAIL (prefix: $PREFIX)"
      echo "AWS Account: $ACCOUNT_ID"
      echo "Using profile: $PROFILE"
      echo "Using region:  $REGION"
      echo "Using key:     $KEY_NAME"
      echo "Using AZs:     $AVAILABLE_AZS"

      # Pre-flight quota check - fail fast before creating any infrastructure
      echo ""
      echo "🔍 Running pre-flight quota validation..."
      PREFLIGHT_OUTPUT=$(uv run python -c "
import asyncio
from cybersec.aws.quota import check_deployment_quotas

async def main():
    result = await check_deployment_quotas('$REGION', required_eips=1, required_vpcs=1)
    print(result.format_report())
    return 0 if result.success else 1

exit(asyncio.run(main()))
" 2>&1) || PREFLIGHT_FAILED=1

      echo "$PREFLIGHT_OUTPUT"
      echo ""

      if [ "''${PREFLIGHT_FAILED:-0}" = "1" ]; then
        echo "❌ Pre-flight quota check FAILED. Provisioning blocked."
        echo ""
        echo "Fix quota issues before provisioning:"
        echo "  - Release unused EIPs shown above"
        echo "  - Or request a quota increase from AWS"
        echo ""
        echo "Run 'cyberphy \"/aws preflight $REGION\"' for details."
        exit 1
      fi

      echo "✅ Pre-flight quota check PASSED"
      echo ""

      # Clear stale state locks (no running tofu/terraform)
      LOCK_FILE=".terraform.tfstate.lock.info"
      if [ -f "$LOCK_FILE" ]; then
        if pgrep -f "tofu|terraform" >/dev/null 2>&1; then
          echo "❌ Detected running tofu/terraform with an active lock."
          echo "   Wait for it to finish or terminate it before retrying."
          exit 1
        fi

        LOCK_ID=$(python - <<'PY'
import json
from pathlib import Path
path = Path(".terraform.tfstate.lock.info")
try:
    data = json.loads(path.read_text())
    print(data.get("ID", ""))
except Exception:
    print("")
PY
)
        echo "🔓 Clearing stale state lock..."
        if [ -n "$LOCK_ID" ]; then
          tofu force-unlock -force "$LOCK_ID" || rm -f "$LOCK_FILE"
        else
          rm -f "$LOCK_FILE"
        fi
      fi

      if [ ! -f .terraform.lock.hcl ]; then
        echo "Initializing Tofu..."
        tofu init
      fi

      echo ""
      echo "Running tofu plan..."
      (
        tofu plan -out=tfplan -var "ssh_key_name=$KEY_NAME"
      ) &
      PLAN_PID=$!
      START_TS=$(date +%s)
      while kill -0 "$PLAN_PID" 2>/dev/null; do
        ELAPSED=$(( $(date +%s) - START_TS ))
        echo "...tofu plan running (''${ELAPSED}s elapsed)"
        sleep 30
      done
      wait "$PLAN_PID"
      PLAN_EXIT=$?

      if [ "$PLAN_EXIT" -ne 0 ]; then
        echo ""
        echo "❌ tofu plan failed."
        echo ""
        echo "If you see 'Inconsistent dependency lock file', run:"
        echo "   cd infra/aws/tofu && tofu init -upgrade"
        echo ""
        echo "Then retry: devenv tasks run aws:provision"
        exit 1
      fi

      # Generate JSON plan for policy validation
      echo ""
      echo "Validating against OPA policies..."
      tofu show -json tfplan > tfplan.json

      # Check for existing S3 bucket and get its region (for cross-region detection)
      BUCKET_NAME="cybersec-dask-$PREFIX-data"
      S3_BUCKET_REGION=$(aws s3api get-bucket-location --bucket "$BUCKET_NAME" --query 'LocationConstraint' --output text 2>/dev/null || echo "")
      # AWS returns "None" for us-east-1 buckets (legacy behavior)
      if [ "$S3_BUCKET_REGION" = "None" ] || [ "$S3_BUCKET_REGION" = "null" ]; then
        S3_BUCKET_REGION="us-east-1"
      fi

      # Check if SSH key pair exists in target region
      SSH_KEY_EXISTS="false"
      if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" >/dev/null 2>&1; then
        SSH_KEY_EXISTS="true"
      fi

      # Create policy input with plan and context
      jq -n --slurpfile plan tfplan.json \
            --arg prefix "$PREFIX" \
            --arg email "$EMAIL" \
            --arg account "$ACCOUNT_ID" \
            --arg region "$REGION" \
            --arg s3_bucket_region "$S3_BUCKET_REGION" \
            --argjson ssh_key_exists "$SSH_KEY_EXISTS" \
            '{plan: $plan[0], context: {developer_prefix: $prefix, developer_email: $email, aws_account_id: $account, aws_region: $region, s3_bucket_region: $s3_bucket_region, ssh_key_exists: $ssh_key_exists, operation: "provision"}}' \
        > policy_input.json

      # Validate with conftest
      echo ""
      if ! conftest test policy_input.json --policy ../../../policy/tofu/ --namespace tofu.provision --all-namespaces; then
        echo ""
        echo "❌ Policy validation FAILED. Provisioning blocked."
        echo ""
        echo "The provision operation was blocked due to policy violations."
        echo "Check that resources have correct Owner tags and naming conventions."
        rm -f tfplan tfplan.json policy_input.json
        exit 1
      fi

      echo ""
      echo "✅ Policy validation PASSED"
      echo ""
      echo "Applying plan..."
      (
        # Unset env var to avoid "Mismatch between input and plan variable value" error
        unset TF_VAR_ssh_key_name
        tofu apply "tfplan"
      ) &
      APPLY_PID=$!
      START_TS=$(date +%s)
      while kill -0 "$APPLY_PID" 2>/dev/null; do
        ELAPSED=$(( $(date +%s) - START_TS ))
        echo "...tofu apply running (''${ELAPSED}s elapsed)"
        sleep 30
      done
      wait "$APPLY_PID"

      # Cleanup temporary files
      rm -f tfplan tfplan.json policy_input.json

      echo ""
      echo "✅ Infrastructure provisioned"
      echo ""
      echo "Next steps:"
      echo "  1. Update inventory: devenv tasks run aws:inventory"
      echo "  2. Deploy cluster: devenv tasks run aws:deploy"
    '';

    "aws:destroy".exec = ''
      echo "⚠️  Destroying AWS infrastructure..."
      echo ""
      echo "💡 Monitor progress in another terminal:"
      echo "   ./scripts/monitor_aws.sh --watch"
      echo ""
      cd infra/aws/tofu

      # Get developer identity and AWS config
      PROFILE="''${AWS_PROFILE:-default}"

      # Auto-detect region from tfstate (resources have ARNs with region)
      # This ensures destroy targets the same region where resources were created
      STATE_REGION=""
      if [ -f terraform.tfstate ]; then
        STATE_REGION=$(python3 - <<'PY'
import json, re
try:
    state = json.load(open("terraform.tfstate"))
    for resource in state.get("resources", []):
        for instance in resource.get("instances", []):
            attrs = instance.get("attributes", {})
            # Check ARN fields for region (arn:aws:service:REGION:account:...)
            for key in ["arn", "id"]:
                val = attrs.get(key, "")
                if isinstance(val, str) and val.startswith("arn:aws:"):
                    match = re.search(r"arn:aws:[^:]+:([a-z]{2}-[a-z]+-\d+):", val)
                    if match:
                        print(match.group(1))
                        exit(0)
except Exception:
    pass
PY
)
      fi

      if [ -n "$STATE_REGION" ]; then
        REGION="$STATE_REGION"
        echo "📍 Detected region from tfstate: $REGION"
      else
        REGION="''${AWS_REGION:-$(aws configure get region --profile "$PROFILE" 2>/dev/null || echo "us-east-1")}"
      fi
      PREFIX=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_prefix; settings = SettingsManager(); config = settings.load(); print(get_developer_prefix(config))")
      EMAIL=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_email; settings = SettingsManager(); config = settings.load(); print(get_developer_email(config))")
      ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
      KEY_NAME="cybersec-dask-$PREFIX"

      # Export TF variables
      export TF_VAR_developer_prefix="$PREFIX"
      export TF_VAR_developer_email="$EMAIL"
      export TF_VAR_ssh_key_name="$KEY_NAME"
      export TF_VAR_aws_region="$REGION"
      # Pin provider to the operator's profile so destroy targets the same
      # account as the prior apply (see provider.tf comment).
      export TF_VAR_aws_profile="$PROFILE"

      # Ingress provider: cloudflare (default) or ngrok
      export TF_VAR_ingress_provider="''${INGRESS_PROVIDER:-cloudflare}"
      export TF_VAR_cloudflare_account_id="''${CLOUDFLARE_ACCOUNT_ID:-}"
      export TF_VAR_cloudflare_zone_id="''${CLOUDFLARE_ZONE_ID:-}"

      # Dynamically detect available AZs (some regions like us-west-1 only have 2)
      AVAILABLE_AZS=$(aws ec2 describe-availability-zones --region "$REGION" --query 'AvailabilityZones[?State==`available`].ZoneName' --output json 2>/dev/null || echo '[]')
      if [ "$AVAILABLE_AZS" = "[]" ]; then
        # Fallback to common pattern if API call fails
        AVAILABLE_AZS="[\"''${REGION}a\",\"''${REGION}b\",\"''${REGION}c\"]"
      fi
      export TF_VAR_availability_zones="$AVAILABLE_AZS"

      echo "Developer: $EMAIL (prefix: $PREFIX)"
      echo "AWS Account: $ACCOUNT_ID"
      echo "Using region:  $REGION"
      echo ""

      # Clear stale state locks (no running tofu/terraform)
      LOCK_FILE=".terraform.tfstate.lock.info"
      if [ -f "$LOCK_FILE" ]; then
        if pgrep -f "tofu|terraform" >/dev/null 2>&1; then
          echo "❌ Detected running tofu/terraform with an active lock."
          echo "   Wait for it to finish or terminate it before retrying."
          exit 1
        fi

        LOCK_ID=$(python - <<'PY'
import json
from pathlib import Path
path = Path(".terraform.tfstate.lock.info")
try:
    data = json.loads(path.read_text())
    print(data.get("ID", ""))
except Exception:
    print("")
PY
)
        echo "🔓 Clearing stale state lock..."
        if [ -n "$LOCK_ID" ]; then
          tofu force-unlock -force "$LOCK_ID" || rm -f "$LOCK_FILE"
        else
          rm -f "$LOCK_FILE"
        fi
      fi

      # Ensure providers are initialized (lock file may exist but .terraform/ is gitignored)
      tofu init -input=false -no-color >/dev/null 2>&1 || true

      echo "Generating destroy plan..."
      (
        tofu plan -destroy -var "ssh_key_name=$KEY_NAME" -out=destroy.tfplan
      ) &
      PLAN_PID=$!
      START_TS=$(date +%s)
      while kill -0 "$PLAN_PID" 2>/dev/null; do
        ELAPSED=$(( $(date +%s) - START_TS ))
        echo "...tofu destroy plan running (''${ELAPSED}s elapsed)"
        sleep 30
      done
      wait "$PLAN_PID"

      # Generate JSON plan for policy validation
      echo ""
      echo "Validating against OPA policies..."
      tofu show -json destroy.tfplan > destroy.tfplan.json

      # Create policy input with plan and context
      jq -n --slurpfile plan destroy.tfplan.json \
            --arg prefix "$PREFIX" \
            --arg email "$EMAIL" \
            --arg account "$ACCOUNT_ID" \
            '{plan: $plan[0], context: {developer_prefix: $prefix, developer_email: $email, aws_account_id: $account, operation: "destroy"}}' \
        > policy_input.json

      # Validate with conftest
      echo ""
      if ! conftest test policy_input.json --policy ../../../policy/tofu/ --namespace tofu.destroy --all-namespaces; then
        echo ""
        echo "❌ Policy validation FAILED. Destruction blocked."
        echo ""
        echo "The destroy operation was blocked because resources don't match your developer identity."
        echo "If you believe this is an error, check the Owner tags on the resources."
        rm -f destroy.tfplan destroy.tfplan.json policy_input.json
        exit 1
      fi

      echo ""
      echo "✅ Policy validation PASSED"
      echo ""
      echo "Applying destruction..."
      (
        tofu destroy -auto-approve
      ) &
      DESTROY_PID=$!
      START_TS=$(date +%s)
      while kill -0 "$DESTROY_PID" 2>/dev/null; do
        ELAPSED=$(( $(date +%s) - START_TS ))
        echo "...tofu destroy running (''${ELAPSED}s elapsed)"
        sleep 30
      done
      wait "$DESTROY_PID"

      # Cleanup temporary files
      rm -f destroy.tfplan destroy.tfplan.json policy_input.json

      echo "✅ Infrastructure destroyed"
    '';

    # Complete teardown - empties S3 bucket and destroys all infrastructure
    # Use this to avoid overnight AWS costs
    "aws:teardown".exec = ''
      echo "🗑️  COMPLETE AWS TEARDOWN"
      echo "========================="
      echo ""
      echo "This will:"
      echo "  1. Delete ALL data from the S3 bucket (including all versions)"
      echo "  2. Destroy ALL AWS infrastructure (EC2, VPC, IAM, etc.)"
      echo ""

      cd infra/aws/tofu

      # Get developer identity
      PROFILE="''${AWS_PROFILE:-default}"
      PREFIX=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_prefix; settings = SettingsManager(); config = settings.load(); print(get_developer_prefix(config))")
      EMAIL=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_email; settings = SettingsManager(); config = settings.load(); print(get_developer_email(config))")
      ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")

      echo "Developer: $EMAIL (prefix: $PREFIX)"
      echo "AWS Account: $ACCOUNT_ID"
      echo "AWS Profile: $PROFILE"
      echo ""

      # Auto-detect region from tfstate (resources have ARNs with region).
      # Mirrors aws:destroy. Critical for ownership isolation: AWS_REGION env
      # MUST NOT be allowed to silently retarget a teardown at a different
      # region than the one the local state actually represents.
      STATE_REGION=""
      if [ -f terraform.tfstate ]; then
        STATE_REGION=$(python3 - <<'PY'
import json, re
try:
    state = json.load(open("terraform.tfstate"))
    for resource in state.get("resources", []):
        for instance in resource.get("instances", []):
            attrs = instance.get("attributes", {})
            for key in ["arn", "id"]:
                val = attrs.get(key, "")
                if isinstance(val, str) and val.startswith("arn:aws:"):
                    match = re.search(r"arn:aws:[^:]+:([a-z]{2}-[a-z]+-\d+):", val)
                    if match:
                        print(match.group(1))
                        exit(0)
except Exception:
    pass
PY
)
      fi

      if [ -n "$STATE_REGION" ]; then
        REGION="$STATE_REGION"
        echo "📍 Detected region from tfstate: $REGION"
      else
        REGION="''${AWS_REGION:-$(aws configure get region 2>/dev/null || echo "us-east-1")}"
      fi
      KEY_NAME="cybersec-dask-$PREFIX"

      # Export TF variables required for plan
      export TF_VAR_developer_prefix="$PREFIX"
      export TF_VAR_developer_email="$EMAIL"
      export TF_VAR_ssh_key_name="$KEY_NAME"
      export TF_VAR_aws_region="$REGION"
      # Pin provider to the operator's profile so teardown targets the same
      # account as the prior apply (see provider.tf comment).
      export TF_VAR_aws_profile="$PROFILE"

      # Ingress provider settings
      export TF_VAR_ingress_provider="''${INGRESS_PROVIDER:-cloudflare}"
      export TF_VAR_cloudflare_account_id="''${CLOUDFLARE_ACCOUNT_ID:-}"
      export TF_VAR_cloudflare_zone_id="''${CLOUDFLARE_ZONE_ID:-}"

      # Dynamically detect available AZs (some regions like us-west-1 only have 2)
      AVAILABLE_AZS=$(aws ec2 describe-availability-zones --region "$REGION" --query 'AvailabilityZones[?State==`available`].ZoneName' --output json 2>/dev/null || echo '[]')
      if [ "$AVAILABLE_AZS" = "[]" ]; then
        AVAILABLE_AZS="[\"''${REGION}a\",\"''${REGION}b\",\"''${REGION}c\"]"
      fi
      export TF_VAR_availability_zones="$AVAILABLE_AZS"

      # Get bucket name from Tofu state
      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      OBJECT_COUNT=0
      BUCKET_OWNER=""

      if [ -z "$BUCKET_NAME" ]; then
        echo "No S3 bucket found in Tofu state."
        echo "Proceeding with infrastructure destroy only..."
      else
        echo "S3 Bucket: $BUCKET_NAME"

        # Check bucket contents
        OBJECT_COUNT=$(aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --query 'length(Contents || `[]`)' --output text 2>/dev/null || echo "0")
        BUCKET_OWNER=$(aws s3api get-bucket-tagging --bucket "$BUCKET_NAME" --query 'TagSet[?Key==`Owner`].Value | [0]' --output text 2>/dev/null || echo "")
        echo "Objects in bucket: $OBJECT_COUNT"
        if [ -n "$BUCKET_OWNER" ]; then
          echo "Bucket owner: $BUCKET_OWNER"
        fi
        echo ""
      fi

      # Generate destroy plan for policy validation
      echo "Generating destroy plan..."
      tofu plan -destroy -out=destroy.tfplan
      tofu show -json destroy.tfplan > destroy.tfplan.json

      # Create policy input with plan and S3 context
      echo ""
      echo "Validating against OPA policies..."
      jq -n --slurpfile plan destroy.tfplan.json \
            --arg prefix "$PREFIX" \
            --arg email "$EMAIL" \
            --arg account "$ACCOUNT_ID" \
            --arg bucket "$BUCKET_NAME" \
            --argjson objects "$OBJECT_COUNT" \
            --arg owner "$BUCKET_OWNER" \
            '{plan: $plan[0], context: {developer_prefix: $prefix, developer_email: $email, aws_account_id: $account, s3_bucket: $bucket, s3_object_count: $objects, bucket_owner: $owner, operation: "teardown"}}' \
        > policy_input.json

      # Validate with conftest (teardown policy includes both destroy and s3_cleanup)
      echo ""
      if ! conftest test policy_input.json --policy ../../../policy/tofu/ --namespace tofu.teardown --all-namespaces; then
        echo ""
        echo "❌ Policy validation FAILED. Teardown blocked."
        echo ""
        echo "The teardown operation was blocked because resources don't match your developer identity."
        echo "If you believe this is an error, check the Owner tags on the resources."
        rm -f destroy.tfplan destroy.tfplan.json policy_input.json
        exit 1
      fi

      echo ""
      echo "✅ Policy validation PASSED"

      # Step 1: Empty S3 bucket (required before Tofu can delete it)
      if [ -n "$BUCKET_NAME" ]; then
        echo ""
        echo "Step 1/2: Emptying S3 bucket..."

        # Delete all object versions (required for versioned buckets)
        echo "  Deleting all object versions..."
        aws s3api list-object-versions --bucket "$BUCKET_NAME" --output json 2>/dev/null | \
          jq -r '.Versions[]? | "\(.Key) \(.VersionId)"' | \
          while read key version; do
            [ -n "$key" ] && aws s3api delete-object --bucket "$BUCKET_NAME" --key "$key" --version-id "$version" 2>/dev/null
          done

        # Delete all delete markers
        echo "  Deleting delete markers..."
        aws s3api list-object-versions --bucket "$BUCKET_NAME" --output json 2>/dev/null | \
          jq -r '.DeleteMarkers[]? | "\(.Key) \(.VersionId)"' | \
          while read key version; do
            [ -n "$key" ] && aws s3api delete-object --bucket "$BUCKET_NAME" --key "$key" --version-id "$version" 2>/dev/null
          done

        # Final cleanup with aws s3 rm (catches anything missed)
        aws s3 rm "s3://$BUCKET_NAME" --recursive 2>/dev/null || true

        echo "  ✅ S3 bucket emptied"
      fi

      # Step 2: Clean up RKE2-created resources not managed by Tofu
      echo ""
      echo "Step 2/3: Cleaning up RKE2-created resources..."

      # Delete NLB created by RKE2 (blocks subnet deletion if not removed)
      NLB_NAME="cybersec-dask-k8s-api"
      NLB_ARN=$(aws elbv2 describe-load-balancers --names "$NLB_NAME" --region "$REGION" --query 'LoadBalancers[0].LoadBalancerArn' --output text 2>/dev/null || echo "")
      if [ -n "$NLB_ARN" ] && [ "$NLB_ARN" != "None" ]; then
        echo "  Deleting NLB: $NLB_NAME"
        aws elbv2 delete-load-balancer --load-balancer-arn "$NLB_ARN" --region "$REGION"
        echo "  Waiting for NLB deletion (up to 2 min)..."
        aws elbv2 wait load-balancers-deleted --load-balancer-arns "$NLB_ARN" --region "$REGION" 2>/dev/null || sleep 30
        echo "  ✅ NLB deleted"
      else
        echo "  No RKE2 NLB found (already deleted or not created)"
      fi

      # Step 3: Destroy infrastructure
      echo ""
      echo "Step 3/3: Destroying infrastructure with Tofu..."
      tofu destroy -auto-approve

      # Cleanup temporary files
      rm -f destroy.tfplan destroy.tfplan.json policy_input.json

      echo ""
      echo "✅ TEARDOWN COMPLETE"
      echo ""
      echo "All AWS resources have been destroyed."
      echo "No further charges will be incurred for this infrastructure."
    '';

    # Just clean S3 data without destroying infrastructure
    "aws:s3:clean".exec = ''
      echo "🧹 Cleaning S3 validation data..."
      cd infra/aws/tofu

      # Get developer identity
      PREFIX=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_prefix; settings = SettingsManager(); config = settings.load(); print(get_developer_prefix(config))")
      EMAIL=$(uv run python -c "from cybersec.bootstrap.config import SettingsManager; from cybersec.bootstrap.identity import get_developer_email; settings = SettingsManager(); config = settings.load(); print(get_developer_email(config))")

      echo "Developer: $EMAIL (prefix: $PREFIX)"
      echo ""

      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")

      if [ -z "$BUCKET_NAME" ]; then
        echo "❌ No S3 bucket found in Tofu state."
        exit 1
      fi

      echo "Bucket: $BUCKET_NAME"

      # Get bucket metadata for policy validation
      OBJECT_COUNT=$(aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --query 'length(Contents || `[]`)' --output text 2>/dev/null || echo "0")
      BUCKET_OWNER=$(aws s3api get-bucket-tagging --bucket "$BUCKET_NAME" --query 'TagSet[?Key==`Owner`].Value | [0]' --output text 2>/dev/null || echo "")

      echo "Objects: $OBJECT_COUNT"
      if [ -n "$BUCKET_OWNER" ]; then
        echo "Owner: $BUCKET_OWNER"
      fi
      echo ""

      # Create policy input for S3 cleanup validation
      echo "Validating against OPA policies..."
      jq -n --arg prefix "$PREFIX" \
            --arg email "$EMAIL" \
            --arg bucket "$BUCKET_NAME" \
            --argjson objects "$OBJECT_COUNT" \
            --arg owner "$BUCKET_OWNER" \
            '{context: {developer_prefix: $prefix, developer_email: $email, s3_bucket: $bucket, s3_object_count: $objects, bucket_owner: $owner, operation: "s3_clear"}}' \
        > policy_input.json

      # Validate with conftest
      if ! conftest test policy_input.json --policy ../../../policy/tofu/s3_cleanup.rego --all-namespaces; then
        echo ""
        echo "❌ Policy validation FAILED. S3 cleanup blocked."
        echo ""
        echo "The cleanup operation was blocked because the bucket doesn't match your developer identity."
        rm -f policy_input.json
        exit 1
      fi

      echo ""
      echo "✅ Policy validation PASSED"
      echo ""
      echo "Listing data directories..."
      aws s3 ls "s3://$BUCKET_NAME/" 2>/dev/null || true
      echo ""

      echo "Deleting objects (this may take a while for large datasets)..."

      # Delete all object versions
      aws s3api list-object-versions --bucket "$BUCKET_NAME" --output json 2>/dev/null | \
        jq -r '.Versions[]? | "\(.Key)\t\(.VersionId)"' | \
        while IFS=$'\t' read key version; do
          [ -n "$key" ] && aws s3api delete-object --bucket "$BUCKET_NAME" --key "$key" --version-id "$version" 2>/dev/null
        done

      # Delete delete markers
      aws s3api list-object-versions --bucket "$BUCKET_NAME" --output json 2>/dev/null | \
        jq -r '.DeleteMarkers[]? | "\(.Key)\t\(.VersionId)"' | \
        while IFS=$'\t' read key version; do
          [ -n "$key" ] && aws s3api delete-object --bucket "$BUCKET_NAME" --key "$key" --version-id "$version" 2>/dev/null
        done

      # Cleanup temporary files
      rm -f policy_input.json

      echo "✅ S3 bucket cleaned"
    '';

    # Show developer identity and AWS configuration
    "aws:identity".exec = ''
      uv run cyberphy "/aws"
    '';

    # Show active AWS profile/region and validate credentials
    "aws:profile".exec = ''
      PROFILE="''${AWS_PROFILE:-default}"
      REGION="''${AWS_REGION:-$(aws configure get region --profile "$PROFILE" 2>/dev/null || echo "us-east-1")}"

      echo "AWS Profile"
      echo "==========="
      echo "Profile: $PROFILE"
      echo "Region:  $REGION"
      echo ""

      if aws sts get-caller-identity --profile "$PROFILE" --region "$REGION" >/dev/null 2>&1; then
        echo "✅ Credentials OK"
      else
        echo "❌ Credentials not valid for profile '$PROFILE'"
        echo "   Run: aws configure --profile $PROFILE"
        exit 1
      fi
    '';

    # Ensure EC2 key pair exists locally and in AWS for RKE2 access
    # Usage:
    #   AWS_PROFILE=default AWS_REGION=us-east-1 devenv tasks run aws:keypair:ensure
    #   devenv tasks run aws:keypair:ensure my-key-name
    "aws:keypair:ensure".exec = ''
      set -euo pipefail

      PROFILE="''${AWS_PROFILE:-default}"
      REGION="''${AWS_REGION:-$(aws configure get region --profile "$PROFILE" 2>/dev/null || echo "us-east-1")}"

      KEY_PATH="$HOME/.ssh/cybersec-dask.pem"

      # Derive a stable key name from developer prefix (matches bucket isolation)
      PREFIX=$(uv run python - <<'PY'
from cybersec.bootstrap.config import SettingsManager
from cybersec.bootstrap.identity import get_developer_prefix

settings = SettingsManager()
config = settings.load()
print(get_developer_prefix(config))
PY
)
      KEY_NAME="''${1:-cybersec-dask-$PREFIX}"

      echo "🔐 Ensuring EC2 key pair"
      echo "  Profile:  $PROFILE"
      echo "  Region:   $REGION"
      echo "  Key name: $KEY_NAME"
      echo "  Key path: $KEY_PATH"

      mkdir -p "$(dirname "$KEY_PATH")"

      LOCAL_EXISTS="false"
      AWS_EXISTS="false"

      if [ -f "$KEY_PATH" ]; then
        LOCAL_EXISTS="true"
      fi

      if aws ec2 describe-key-pairs \
        --profile "$PROFILE" \
        --region "$REGION" \
        --key-names "$KEY_NAME" >/dev/null 2>&1; then
        AWS_EXISTS="true"
      fi

      echo "  Local key: $LOCAL_EXISTS"
      echo "  AWS key:   $AWS_EXISTS"
      echo ""

      if [ "$LOCAL_EXISTS" = "true" ] && [ "$AWS_EXISTS" = "true" ]; then
        echo "✅ Key pair exists both locally and in AWS ($REGION)"
        exit 0
      fi

      if [ "$LOCAL_EXISTS" = "true" ] && [ "$AWS_EXISTS" = "false" ]; then
        echo "Importing local key to AWS ($REGION)..."
        aws ec2 import-key-pair \
          --profile "$PROFILE" \
          --region "$REGION" \
          --key-name "$KEY_NAME" \
          --public-key-material fileb://<(ssh-keygen -y -f "$KEY_PATH")
        echo "✅ Key pair imported to AWS ($REGION)"
        exit 0
      fi

      if [ "$LOCAL_EXISTS" = "false" ] && [ "$AWS_EXISTS" = "true" ]; then
        echo "❌ Key pair '$KEY_NAME' exists in AWS ($REGION), but no local file at $KEY_PATH"
        echo "   Either copy the PEM file into place or delete the AWS key pair:"
        echo "   aws ec2 delete-key-pair --key-name $KEY_NAME --region $REGION"
        exit 1
      fi

      # Neither exists - create new
      echo "Creating new key pair..."
      aws ec2 create-key-pair \
        --profile "$PROFILE" \
        --region "$REGION" \
        --key-name "$KEY_NAME" \
        --query 'KeyMaterial' \
        --output text > "$KEY_PATH"

      chmod 600 "$KEY_PATH"
      echo "✅ Key pair created and saved to $KEY_PATH"
    '';

    # Bootstrap AWS dev config for K8s (region + identity + prepare)
    "aws:setup".exec = ''
      echo "🔧 AWS setup (region + identity + K8s prep)"
      echo "======================================"

      echo "Setting AWS region to us-east-1..."
      uv run cyberphy "/aws target us-east-1"

      echo ""
      echo "Developer identity:"
      devenv tasks run aws:identity

      echo ""
      echo "Ensuring EC2 key pair:"
      devenv tasks run aws:keypair:ensure

      echo ""
      echo "Preparing AWS K8s target..."
      devenv tasks run k8s:prepare-aws
    '';

    # Verify S3 bucket ownership tags before operations
    # Uses developer prefix and ownership tags for safety
    "aws:s3:verify".exec = ''
      BUCKET="''${1:-}"
      if [ -n "$BUCKET" ]; then
        uv run cyberphy "/aws s3:verify $BUCKET"
      else
        uv run cyberphy "/aws s3:verify"
      fi
    '';

    # Empty S3 bucket with tag verification for safety
    # Verifies ManagedBy=opentofu and Owner tags to prevent accidents
    # Usage: devenv tasks run aws:s3:empty [bucket] [--apply] [--skip-verify]
    "aws:s3:empty".exec = ''
      # Parse arguments
      BUCKET=""
      APPLY=""
      SKIP_VERIFY=""

      for arg in "$@"; do
        case "$arg" in
          --apply) APPLY="--apply" ;;
          --skip-verify) SKIP_VERIFY="--skip-verify" ;;
          -*) echo "Unknown option: $arg"; exit 1 ;;
          *) BUCKET="$arg" ;;
        esac
      done

      # Build command
      CMD="/aws s3:empty"
      [ -n "$BUCKET" ] && CMD="$CMD $BUCKET"
      [ -n "$APPLY" ] && CMD="$CMD --apply"
      [ -n "$SKIP_VERIFY" ] && CMD="$CMD --skip-verify"

      uv run cyberphy "$CMD"
    '';

    # =========================================================================
    # AWS Target Region Configuration
    # =========================================================================
    # These tasks manage AWS region selection with conftest validation.
    # Flow: validate region -> update .cybersec/config.toml

    # Show current AWS target configuration
    "aws:target".exec = ''
      uv run cyberphy "/aws target"
    '';

    # Set AWS target region with validation
    # Usage: devenv tasks run aws:target:set us-west-1
    "aws:target:set".exec = ''
      REGION="''${1:-}"
      if [ -z "$REGION" ]; then
        echo "Usage: devenv tasks run aws:target:set <region>"
        echo ""
        echo "Allowed regions:"
        echo "  us-east-1, us-east-2, us-west-1, us-west-2"
        echo "  eu-west-1, eu-central-1, ap-southeast-1"
        echo ""
        echo "Run 'devenv tasks run aws:target:list' for full list."
        exit 1
      fi
      uv run cyberphy "/aws target $REGION"
    '';

    # Validate current AWS target without changes (dry-run)
    "aws:target:validate".exec = ''
      REGION="''${1:-}"
      if [ -n "$REGION" ]; then
        uv run cyberphy "/aws target $REGION --dry-run"
      else
        uv run cyberphy "/aws target --dry-run"
      fi
    '';

    # List allowed AWS regions
    "aws:target:list".exec = ''
      uv run cyberphy "/aws target --list"
    '';

    "aws:inventory".exec = ''
      echo "📋 Generating Ansible inventory from Tofu outputs..."
      cd infra/aws/tofu

      # Get outputs
      BASTION_IP=$(tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CONTROL_PLANE_IPS=$(tofu output -json control_plane_private_ips 2>/dev/null || echo "[]")
      WORKER_IPS=$(tofu output -json worker_private_ips 2>/dev/null || echo "[]")
      K8S_API=$(tofu output -raw k8s_api_endpoint 2>/dev/null || echo "")
      REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')

      if [ -z "$BASTION_IP" ]; then
        echo "❌ No bastion IP found. Run 'devenv tasks run aws:provision' first."
        exit 1
      fi

      INVENTORY_FILE="../ansible/inventory/hosts"
      cat > "$INVENTORY_FILE" << EOF
# Ansible inventory generated by OpenTofu
# SSH to bastion: ssh -i ~/.ssh/cybersec-dask.pem ec2-user@$BASTION_IP
# SSH to internal: ssh -J ec2-user@$BASTION_IP ec2-user@<private_ip>

[bastion]
$BASTION_IP ansible_user=ec2-user ansible_host=$BASTION_IP

[control_plane]
EOF

      # Add control plane nodes
      echo "$CONTROL_PLANE_IPS" | jq -r '.[] // empty' | nl -v 1 | while read num ip; do
        echo "$ip ansible_user=ec2-user rke2_type=server node_name=control-plane-$num" >> "$INVENTORY_FILE"
      done

      cat >> "$INVENTORY_FILE" << EOF

[workers]
EOF

      # Add worker nodes
      echo "$WORKER_IPS" | jq -r '.[] // empty' | nl -v 1 | while read num ip; do
        echo "$ip ansible_user=ec2-user rke2_type=agent node_name=worker-$num" >> "$INVENTORY_FILE"
      done

      cat >> "$INVENTORY_FILE" << EOF

[rke2:children]
control_plane
workers

[rke2:vars]
ansible_ssh_private_key_file=~/.ssh/cybersec-dask.pem
ansible_ssh_common_args='-o ProxyCommand="ssh -i ~/.ssh/cybersec-dask.pem -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP" -o StrictHostKeyChecking=no'
k8s_api_endpoint=$K8S_API

[bastion:vars]
ansible_ssh_private_key_file=~/.ssh/cybersec-dask.pem
ansible_ssh_common_args='-o StrictHostKeyChecking=no'
aws_region=$REGION
EOF

      echo "✅ Inventory written to $INVENTORY_FILE"
      echo "   Region: $REGION"
      echo ""
      cat "$INVENTORY_FILE"
    '';

    "aws:status".exec = ''
      echo "📊 AWS Cluster Status"
      echo "====================="
      echo ""

      # Check tofu state
      if [ -f infra/aws/tofu/terraform.tfstate ]; then
        cd infra/aws/tofu
        echo "Infrastructure:"
        echo "  Bastion:       $(tofu output -raw bastion_public_ip 2>/dev/null || echo 'N/A')"
        echo "  K8s API:       $(tofu output -raw k8s_api_endpoint 2>/dev/null || echo 'N/A')"
        echo "  Control Planes: $(tofu output -json control_plane_private_ips 2>/dev/null | jq -r 'length' || echo '0')"
        echo "  Workers:       $(tofu output -json worker_private_ips 2>/dev/null | jq -r 'length' || echo '0')"
        cd - > /dev/null
      else
        echo "Infrastructure: Not provisioned"
      fi

      echo ""

      # Check connectivity
      BASTION_IP=$(cd infra/aws/tofu && tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      if [ -n "$BASTION_IP" ]; then
        echo "Connectivity:"
        if timeout 10 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i ~/.ssh/cybersec-dask.pem ec2-user@$BASTION_IP "echo OK" 2>/dev/null; then
          echo "  Bastion SSH:   ✅ OK"
        else
          echo "  Bastion SSH:   ❌ Failed"
        fi
      fi
    '';

    "aws:deploy".exec = ''
      echo "🚀 Deploying full RKE2 cluster with Dask..."
      echo ""
      echo "💡 Monitor progress in another terminal:"
      echo "   ./scripts/monitor_deploy.sh --watch"
      echo ""
      export PROJECT_ROOT="$PWD"

      # Override MinIO credentials with real AWS credentials from profile.
      # Use export-credentials so SSO profiles work too — aws configure get
      # only reads static keys and returns empty for SSO profiles.
      CREDS_JSON=$(aws configure export-credentials --profile "''${AWS_PROFILE:-default}" 2>/dev/null || echo '{}')
      export AWS_ACCESS_KEY_ID=$(echo "$CREDS_JSON" | jq -r '.AccessKeyId // empty')
      export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.SecretAccessKey // empty')
      export AWS_SESSION_TOKEN=$(echo "$CREDS_JSON" | jq -r '.SessionToken // empty')

      if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "❌ Failed to resolve AWS credentials for profile ''${AWS_PROFILE:-default}."
        echo "   For SSO profiles: aws sso login --profile ''${AWS_PROFILE:-default}"
        exit 1
      fi

      # Prevent conflict with local MinIO
      unset S3_ENDPOINT

      cd infra/aws/tofu
      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")

      # Get region from tofu state to ensure consistency with provisioned infrastructure
      export AWS_REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      
      if [ -z "$BUCKET_NAME" ]; then
        echo "❌ Error: Could not determine S3 bucket name from Tofu."
        echo "   This usually means the infrastructure has not been provisioned yet."
        echo "   Please run: devenv tasks run aws:provision"
        exit 1
      fi
      
      echo "Using S3 Bucket: $BUCKET_NAME"
      echo "Using Region:    $AWS_REGION"

      # Detect ingress provider from tofu state
      INGRESS_PROVIDER=$(tofu output -json ingress_info 2>/dev/null | jq -r '.provider // "ngrok"')
      export INGRESS_PROVIDER

      if [ "$INGRESS_PROVIDER" = "cloudflare" ]; then
        echo "Using Ingress:   Cloudflare Tunnel (Zero Trust)"
        # Get tunnel credentials from tofu for Ansible
        export CLOUDFLARE_TUNNEL_TOKEN=$(tofu output -raw cloudflare_tunnel_token 2>/dev/null || echo "")
        export CLOUDFLARE_TUNNEL_ID=$(tofu output -raw cloudflare_tunnel_id 2>/dev/null || echo "")
        if [ -z "$CLOUDFLARE_TUNNEL_TOKEN" ]; then
          echo "⚠️  Warning: Could not get tunnel token from tofu output"
        fi
        # Service token for cloudflared access (Zero Trust tunnel auth)
        export CF_ACCESS_CLIENT_ID=$(tofu output -raw cf_access_client_id 2>/dev/null || echo "")
        export CF_ACCESS_CLIENT_SECRET=$(tofu output -raw cf_access_client_secret 2>/dev/null || echo "")
      else
        echo "Using Ingress:   ngrok"
      fi

      cd ../ansible
      ansible-playbook playbooks/site.yml -e "s3_bucket_name=$BUCKET_NAME" -e "aws_region=$AWS_REGION"
      echo ""
      echo "✅ Cluster deployment complete"
      echo ""
      if [ "$INGRESS_PROVIDER" = "cloudflare" ]; then
        INGRESS_URLS=$(cd ../tofu && tofu output -json ingress_urls 2>/dev/null | jq -r 'to_entries[] | "  - \(.key): \(.value)"')
        echo "Access URLs (requires WARP):"
        echo "$INGRESS_URLS"
      else
        echo "Next steps:"
        echo "  - Deploy ngrok:      devenv tasks run aws:deploy:ngrok"
        echo "  - Deploy JupyterHub: devenv tasks run aws:deploy:jupyterhub"
      fi
    '';

    # Deploy via Zarf package (standardized path for air-gap and AWS)
    # Replaces the Ansible-based aws:deploy for application workloads.
    # Infrastructure (RKE2 cluster) is still deployed via Ansible cluster-only.yml.
    "aws:deploy:zarf".exec = ''
      echo "🚀 Deploying Zarf package to AWS RKE2 cluster..."
      echo ""

      export PROJECT_ROOT="$PWD"

      # Get infrastructure info from tofu state
      cd infra/aws/tofu
      BASTION_IP=$(tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CONTROL_IP=$(tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty' || echo "")
      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      AWS_REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      INGRESS_PROVIDER=$(tofu output -json ingress_info 2>/dev/null | jq -r '.provider // "cloudflare"')
      WORKER_NODE_COUNT=$(tofu output -json worker_private_ips 2>/dev/null | jq 'length' 2>/dev/null || echo "")
      cd "$PROJECT_ROOT"

      if [ -z "$BASTION_IP" ] || [ -z "$CONTROL_IP" ]; then
        echo "❌ Cluster not provisioned. Run 'devenv tasks run aws:provision' first."
        exit 1
      fi

      # Get AWS credentials via export-credentials so SSO profiles work too.
      # aws configure get aws_access_key_id only reads static keys from
      # ~/.aws/credentials and returns exit 1 (empty value) for SSO profiles,
      # which kills the task under set -e before the SCP phase even starts.
      CREDS_JSON=$(aws configure export-credentials --profile "''${AWS_PROFILE:-default}" 2>/dev/null || echo '{}')
      AWS_ACCESS_KEY_ID=$(echo "$CREDS_JSON" | jq -r '.AccessKeyId // empty')
      AWS_SECRET_ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.SecretAccessKey // empty')
      AWS_SESSION_TOKEN=$(echo "$CREDS_JSON" | jq -r '.SessionToken // empty')

      if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "❌ Failed to resolve AWS credentials for profile ''${AWS_PROFILE:-default}."
        echo "   For SSO profiles: aws sso login --profile ''${AWS_PROFILE:-default}"
        echo "   For static profiles: confirm ~/.aws/credentials has aws_access_key_id set."
        exit 1
      fi

      SSH_KEY="$HOME/.ssh/cybersec-dask.pem"
      SSH_OPTS="-o StrictHostKeyChecking=no"
      SSH_BASTION="ssh -i $SSH_KEY $SSH_OPTS ec2-user@$BASTION_IP"
      SSH_CP="ssh -i $SSH_KEY -o ProxyCommand=\"ssh -i $SSH_KEY -W %h:%p $SSH_OPTS ec2-user@$BASTION_IP\" $SSH_OPTS ec2-user@$CONTROL_IP"

      echo "Bastion:       $BASTION_IP"
      echo "Control Plane: $CONTROL_IP"
      echo "S3 Bucket:     $BUCKET_NAME"
      echo "Region:        $AWS_REGION"
      echo "Ingress:       $INGRESS_PROVIDER"
      echo ""

      # Find the latest Zarf package
      ZARF_PKG=$(ls -t zarf/zarf-package-cybersec-dask-amd64-*.tar.zst 2>/dev/null | head -1)
      if [ -z "$ZARF_PKG" ]; then
        echo "❌ No Zarf package found. Run: devenv tasks run zarf:package"
        exit 1
      fi
      ZARF_PKG_NAME=$(basename "$ZARF_PKG")
      echo "Package: $ZARF_PKG_NAME ($(du -h "$ZARF_PKG" | cut -f1))"
      echo ""

      # Phase 1a: Stage SSH key on bastion. The bastion needs it to hop to the
      # control plane in phase 2. We previously tried to pass $SSH_KEY through
      # the SSH wrapper, but that path is a laptop-side absolute (e.g.
      # /Users/<you>/.ssh/cybersec-dask.pem) and does not exist on the bastion.
      SSH_KEY_BASENAME=$(basename "$SSH_KEY")
      BASTION_KEY_PATH="/home/ec2-user/.ssh/$SSH_KEY_BASENAME"
      echo "🔐 Staging SSH key on bastion at $BASTION_KEY_PATH..."
      scp -i "$SSH_KEY" $SSH_OPTS "$SSH_KEY" "ec2-user@$BASTION_IP:$BASTION_KEY_PATH"
      eval $SSH_BASTION "chmod 600 $BASTION_KEY_PATH"

      # Phase 1b: Transfer package to bastion
      echo "📦 Phase 1: Transferring package to bastion..."
      scp -i "$SSH_KEY" $SSH_OPTS "$ZARF_PKG" "ec2-user@$BASTION_IP:/var/tmp/$ZARF_PKG_NAME"

      # Phase 2: Transfer package from bastion to control plane (use the key
      # we just staged on the bastion, not the laptop path).
      echo "📦 Phase 2: Transferring package to control plane..."
      eval $SSH_BASTION "scp -i $BASTION_KEY_PATH $SSH_OPTS /var/tmp/$ZARF_PKG_NAME ec2-user@$CONTROL_IP:/var/tmp/$ZARF_PKG_NAME"

      # Phase 2b: Ensure the zarf binary AND its init package are present on
      # the control plane. The cybersec-dask package was built against zarf
      # v0.70.1; pin to that exact version to avoid archive-format skew. CP
      # reaches GitHub via the VPC NAT gateway. Both downloads are idempotent
      # — re-running the task skips work that is already done.
      ZARF_VERSION="v0.70.1"
      ZARF_BIN_URL="https://github.com/zarf-dev/zarf/releases/download/''${ZARF_VERSION}/zarf_''${ZARF_VERSION}_Linux_amd64"
      ZARF_INIT_PKG="zarf-init-amd64-''${ZARF_VERSION}.tar.zst"
      ZARF_INIT_URL="https://github.com/zarf-dev/zarf/releases/download/''${ZARF_VERSION}/''${ZARF_INIT_PKG}"
      echo "🔧 Phase 2b: Ensuring zarf ''${ZARF_VERSION} (binary + init package) on control plane..."
      # Pipe the staging script via stdin instead of eval $SSH_CP "..." with
      # semicolons. eval joins all its args with spaces and re-parses, so any
      # semicolon (or newline followed by another command) inside the quoted
      # script gets re-interpreted as a top-level command separator after
      # the ssh invocation. That made cmd2 in `ssh ... 'cmd1; cmd2'` run
      # LOCALLY instead of on the remote host — exactly the bug we hit when
      # /usr/local/bin/zarf version was being executed on the laptop.
      ssh -i "$SSH_KEY" \
        -o ProxyCommand="ssh -i $SSH_KEY -W %h:%p $SSH_OPTS ec2-user@$BASTION_IP" \
        $SSH_OPTS ec2-user@$CONTROL_IP bash -s <<REMOTE_HEREDOC || {
set -e
if ! /usr/local/bin/zarf version >/dev/null 2>&1; then
  echo "  Installing zarf binary $ZARF_VERSION..."
  curl -sSfL "$ZARF_BIN_URL" -o /tmp/zarf-install
  sudo -n install -m 0755 /tmp/zarf-install /usr/local/bin/zarf
  rm -f /tmp/zarf-install
fi
if [ ! -f "/var/tmp/$ZARF_INIT_PKG" ]; then
  echo "  Downloading zarf init package $ZARF_INIT_PKG..."
  curl -sSfL "$ZARF_INIT_URL" -o "/var/tmp/$ZARF_INIT_PKG"
fi
echo -n "  zarf binary: "
/usr/local/bin/zarf version
echo -n "  init pkg:    "
ls -lh "/var/tmp/$ZARF_INIT_PKG"
REMOTE_HEREDOC
        echo "❌ Failed to stage zarf on control plane."
        echo "   Verify NAT egress from private subnet, then re-run the task."
        exit 1
      }

      # Phases 2c–4 (StorageClass bootstrap → stale-ns clear → zarf init →
      # package deploy) are now driven by the CONVERGENCE ENGINE (zarf/converge):
      # discover → diff target → remediate → fixpoint. Idempotent and recoverable
      # from any partial-failure state — replacing the linear heredocs that failed
      # differently every run. Transport (Phases 1–2b above) already staged the
      # package + zarf binary on the control plane. The StorageClass bootstrap is
      # now registry-free: converge kubectl-applies the BUNDLED local-path manifest
      # against the node-preloaded image, so it NO LONGER pulls v0.0.32 from
      # raw.githubusercontent.com (the egress dependency that broke true air-gap).
      echo "🔄 Phases 2c–4: convergence engine → target state..."
      # S3 creds reach zarf via ZARF_VAR_* env (never argv) — converge-aws.sh
      # stages them to tmpfs. DASK_WORKER_REPLICAS seeds deterministic first-deploy
      # sizing (then T4.workers-capacity caps to live schedulable capacity).
      export S3_ENDPOINT="" \
             S3_BUCKET="$BUCKET_NAME" \
             S3_REGION="$AWS_REGION" \
             S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
             S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
             S3_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
             DASK_WORKER_REPLICAS="''${DASK_WORKER_REPLICAS:-''${WORKER_NODE_COUNT:-4}}"
      bash "$PROJECT_ROOT/zarf/scripts/converge-aws.sh" apply || {
        echo "❌ Convergence did not reach target state — see the status table above."
        echo "   Inspect/heal with: MODE=verify devenv tasks run aws:converge"
        exit 1
      }

      echo ""
      echo "✅ Cluster converged to target state"

      # Phase 5: Deploy Cloudflare tunnel (if applicable)
      if [ "$INGRESS_PROVIDER" = "cloudflare" ]; then
        echo ""
        echo "🌐 Phase 5: Deploying Cloudflare tunnel..."
        cd infra/aws/tofu
        CLOUDFLARE_TUNNEL_TOKEN=$(tofu output -raw cloudflare_tunnel_token 2>/dev/null || echo "")
        CLOUDFLARE_TUNNEL_ID=$(tofu output -raw cloudflare_tunnel_id 2>/dev/null || echo "")
        export CLOUDFLARE_TUNNEL_TOKEN CLOUDFLARE_TUNNEL_ID
        cd "$PROJECT_ROOT/infra/aws/ansible"
        ansible-playbook playbooks/site.yml --tags cloudflare \
          -e "s3_bucket_name=$BUCKET_NAME" \
          -e "aws_region=$AWS_REGION"
        echo "✅ Cloudflare tunnel deployed"
      fi

      echo ""
      echo "✅ Deployment complete!"
      echo ""

      if [ "$INGRESS_PROVIDER" = "cloudflare" ]; then
        INGRESS_URLS=$(cd "$PROJECT_ROOT/infra/aws/tofu" && tofu output -json ingress_urls 2>/dev/null | jq -r 'to_entries[] | "  - \(.key): \(.value)"')
        echo "Access URLs (requires WARP):"
        echo "$INGRESS_URLS"
      fi
      echo ""
      echo "Verify with: devenv tasks run aws:verify"
    '';

    # Drive the live AWS cluster to TARGET STATE with the convergence engine
    # (zarf/converge): discover → diff → remediate → repeat to a fixpoint.
    #   MODE=verify (default) read-only target oracle — reports drift, no changes
    #   MODE=apply            remediate to a fixpoint (Layer-B only; never deletes a
    #                         transported image — that guard is structural)
    #   MODE=dry-run          show what apply WOULD do
    #   MODE=teardown         clean-slate the app stack (registry/SC + images kept);
    #                         turn-key + idempotent — pair with MODE=apply to redeploy
    # Use aws:deploy:zarf for an initial from-scratch deploy; use this to
    # verify/heal/teardown+redeploy an existing one idempotently.
    "aws:converge".exec = ''
      exec bash ${config.devenv.root}/zarf/scripts/converge-aws.sh "''${MODE:-verify}"
    '';

    # Write AWS environment variables to .env file for consistent use across sessions
    # This populates OTEL_S3_BUCKET and AWS_REGION from tofu output
    "aws:env".exec = ''
      echo "📝 Writing AWS environment to .env file..."

      TOFU_DIR="${config.devenv.root}/infra/aws/tofu"
      ENV_FILE="${config.devenv.root}/.env"

      # Get values from tofu output
      BUCKET_NAME=$(cd "$TOFU_DIR" && tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      REGION=$(cd "$TOFU_DIR" && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')

      if [ -z "$BUCKET_NAME" ]; then
        echo "❌ Error: Could not determine S3 bucket name from Tofu."
        echo "   Please run: devenv tasks run aws:provision"
        exit 1
      fi

      # Update or create .env file
      # Remove old values if they exist
      if [ -f "$ENV_FILE" ]; then
        grep -v "^OTEL_S3_BUCKET=" "$ENV_FILE" | grep -v "^AWS_REGION=" > "$ENV_FILE.tmp" && mv "$ENV_FILE.tmp" "$ENV_FILE"
      fi

      # Append new values
      echo "OTEL_S3_BUCKET=$BUCKET_NAME" >> "$ENV_FILE"
      echo "AWS_REGION=$REGION" >> "$ENV_FILE"

      echo "✅ Updated .env file:"
      echo "   OTEL_S3_BUCKET=$BUCKET_NAME"
      echo "   AWS_REGION=$REGION"
      echo ""
      echo "💡 Re-enter devenv shell to pick up changes:"
      echo "   exit && devenv shell"
    '';

    "aws:deploy:dask".exec = ''
      echo "🚀 Deploying Dask operator..."

      # Get region from tofu state
      export AWS_REGION=$(cd infra/aws/tofu && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"

      cd infra/aws/ansible
      ansible-playbook playbooks/dask-only.yml -e "aws_region=$AWS_REGION"
      echo "✅ Dask operator deployed"
    '';

    "aws:deploy:ngrok".exec = ''
      echo "🚀 Deploying ngrok operator with OAuth..."

      # Check required environment variables
      if [ -z "$NGROK_AUTH_TOKEN" ] && [ -z "$NGROK_AUTHTOKEN" ]; then
        echo "❌ NGROK_AUTH_TOKEN or NGROK_AUTHTOKEN environment variable required"
        exit 1
      fi
      if [ -z "$NGROK_API_KEY" ]; then
        echo "❌ NGROK_API_KEY environment variable required"
        exit 1
      fi
      if [ -z "$CLOUDFLARE_API_TOKEN" ]; then
        echo "❌ CLOUDFLARE_API_TOKEN environment variable required"
        exit 1
      fi

      # Get region from tofu state
      export AWS_REGION=$(cd infra/aws/tofu && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"

      cd infra/aws/ansible
      ansible-playbook playbooks/ngrok.yml -e "aws_region=$AWS_REGION"
      echo ""
      echo "✅ ngrok operator deployed"
      echo ""
      echo "Access URLs (after DNS propagation):"
      echo "  - Dask Dashboard: https://dask.zndx.org"
      echo "  - K8s Dashboard:  https://k8s.zndx.org"
    '';

    "aws:deploy:cloudflare".exec = ''
      echo "🚀 Deploying Cloudflare Tunnel..."

      # Get tunnel credentials from tofu output
      cd infra/aws/tofu
      export CLOUDFLARE_TUNNEL_TOKEN=$(tofu output -raw cloudflare_tunnel_token 2>/dev/null || echo "")
      export CLOUDFLARE_TUNNEL_ID=$(tofu output -raw cloudflare_tunnel_id 2>/dev/null || echo "")

      if [ -z "$CLOUDFLARE_TUNNEL_TOKEN" ]; then
        echo "❌ Could not get CLOUDFLARE_TUNNEL_TOKEN from tofu output"
        echo "   Make sure infrastructure was provisioned with INGRESS_PROVIDER=cloudflare"
        exit 1
      fi

      # Get region from tofu state
      export AWS_REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"
      echo "Tunnel ID:    $CLOUDFLARE_TUNNEL_ID"

      export INGRESS_PROVIDER=cloudflare
      cd ../ansible
      ansible-playbook playbooks/cloudflare-tunnel.yml -e "aws_region=$AWS_REGION"
      echo ""
      echo "✅ Cloudflare Tunnel deployed"
      echo ""
      INGRESS_URLS=$(cd ../tofu && tofu output -json ingress_urls 2>/dev/null | jq -r 'to_entries[] | "  - \(.key): \(.value)"')
      echo "Access URLs (requires WARP):"
      echo "$INGRESS_URLS"
    '';

    "aws:deploy:jupyterhub".exec = ''
      echo "🚀 Deploying JupyterHub..."
      export PROJECT_ROOT="$PWD"

      # Override MinIO credentials with real AWS credentials from profile.
      # Use export-credentials so SSO profiles work too — aws configure get
      # only reads static keys and returns empty for SSO profiles.
      CREDS_JSON=$(aws configure export-credentials --profile "''${AWS_PROFILE:-default}" 2>/dev/null || echo '{}')
      export AWS_ACCESS_KEY_ID=$(echo "$CREDS_JSON" | jq -r '.AccessKeyId // empty')
      export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.SecretAccessKey // empty')
      export AWS_SESSION_TOKEN=$(echo "$CREDS_JSON" | jq -r '.SessionToken // empty')

      if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "❌ Failed to resolve AWS credentials for profile ''${AWS_PROFILE:-default}."
        echo "   For SSO profiles: aws sso login --profile ''${AWS_PROFILE:-default}"
        exit 1
      fi

      # Get region from tofu state
      export AWS_REGION=$(cd infra/aws/tofu && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"

      # Ingress provider: cloudflare (default) or ngrok (deprecated)
      INGRESS_PROVIDER="''${INGRESS_PROVIDER:-cloudflare}"
      echo "Using Ingress Provider: $INGRESS_PROVIDER"

      cd infra/aws/ansible
      ansible-playbook playbooks/jupyterhub.yml \
        -e "aws_region=$AWS_REGION" \
        -e "ingress_provider=$INGRESS_PROVIDER"
      echo "✅ JupyterHub deployed"
    '';

    "aws:deploy:panel-viz".exec = ''
      echo "🚀 Deploying Panel visualization service..."
      export PROJECT_ROOT="$PWD"

      # Override MinIO credentials with real AWS credentials from profile.
      # Use export-credentials so SSO profiles work too — aws configure get
      # only reads static keys and returns empty for SSO profiles.
      CREDS_JSON=$(aws configure export-credentials --profile "''${AWS_PROFILE:-default}" 2>/dev/null || echo '{}')
      export AWS_ACCESS_KEY_ID=$(echo "$CREDS_JSON" | jq -r '.AccessKeyId // empty')
      export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.SecretAccessKey // empty')
      export AWS_SESSION_TOKEN=$(echo "$CREDS_JSON" | jq -r '.SessionToken // empty')

      if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "❌ Failed to resolve AWS credentials for profile ''${AWS_PROFILE:-default}."
        echo "   For SSO profiles: aws sso login --profile ''${AWS_PROFILE:-default}"
        exit 1
      fi

      # Get region from tofu state
      export AWS_REGION=$(cd infra/aws/tofu && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"

      BUCKET_NAME=$(cd infra/aws/tofu && tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      if [ -z "$BUCKET_NAME" ]; then
        echo "Error: Could not determine S3 bucket name from Tofu."
        exit 1
      fi

      cd infra/aws/ansible
      ansible-playbook playbooks/panel-viz.yml \
        -e "s3_bucket_name=$BUCKET_NAME" \
        -e "aws_region=$AWS_REGION"
      echo ""
      echo "Panel visualization deployed"

      INGRESS_URLS=$(cd ../tofu && tofu output -json ingress_urls 2>/dev/null | jq -r '.viz // ""')
      if [ -n "$INGRESS_URLS" ]; then
        echo "Access URL: $INGRESS_URLS"
      fi
    '';

    # -------------------------------------------------------------------------
    # AWS Credential Refresh
    # -------------------------------------------------------------------------
    # Refreshes temporary AWS session tokens across all namespaces on the
    # Zarf-deployed RKE2 cluster. Run when S3 access fails due to expired
    # credentials (~5 hour STS token lifetime).
    #
    # Updates: panel-viz Secret, Dask worker/scheduler env, JupyterHub Helm values
    # Also SCPs the latest jupyterhub-values.yaml for mount path fixes.
    #
    # Usage:
    #   devenv tasks run aws:refresh-tokens

    "aws:refresh-tokens".exec = ''
      set -euo pipefail
      echo "=== AWS Credential Refresh ==="
      echo ""

      # 1. Read credentials from local AWS profile
      PROFILE="''${AWS_PROFILE:-default}"
      ACCESS_KEY=$(aws configure get aws_access_key_id --profile "$PROFILE")
      SECRET_KEY=$(aws configure get aws_secret_access_key --profile "$PROFILE")
      SESSION_TOKEN=$(aws configure get aws_session_token --profile "$PROFILE" 2>/dev/null || echo "")

      if [ -z "$ACCESS_KEY" ] || [ -z "$SECRET_KEY" ]; then
        echo "Error: Could not read AWS credentials from profile '$PROFILE'"
        echo "Set AWS_PROFILE or run 'aws configure'"
        exit 1
      fi

      echo "AWS Profile: $PROFILE"
      echo "Access Key:  ''${ACCESS_KEY:0:8}..."
      echo "Session Token: $([ -n "$SESSION_TOKEN" ] && echo "''${SESSION_TOKEN:0:12}... ($(echo -n "$SESSION_TOKEN" | wc -c) chars)" || echo "none")"

      # 2. Get infrastructure IPs from tofu state
      TOFU_DIR="infra/aws/tofu"
      BASTION_IP=$(cd "$TOFU_DIR" && tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CONTROL_IP=$(cd "$TOFU_DIR" && tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty' || echo "")

      if [ -z "$BASTION_IP" ] || [ -z "$CONTROL_IP" ]; then
        echo "Error: Could not get cluster IPs from tofu state"
        exit 1
      fi
      echo "Bastion:     $BASTION_IP"
      echo "Control:     $CONTROL_IP"
      echo ""

      SSH_KEY="$HOME/.ssh/cybersec-dask.pem"
      SSH_CMD="ssh -i $SSH_KEY -o ProxyCommand=\"ssh -i $SSH_KEY -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP\" -o StrictHostKeyChecking=no"
      SCP_CMD="scp -i $SSH_KEY -o ProxyCommand=\"ssh -i $SSH_KEY -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP\" -o StrictHostKeyChecking=no"
      KUBECTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"

      # 3. Write a remote script that does all kubectl/helm work in one SSH call.
      #    This avoids quoting issues with eval+SSH+nested-quotes.
      REMOTE_SCRIPT=$(mktemp)
      cat > "$REMOTE_SCRIPT" << 'REMOTE_HEADER'
      #!/bin/bash
      set -euo pipefail
      KUBECTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"
      HELM="sudo /usr/local/bin/helm"
      REMOTE_HEADER

      cat >> "$REMOTE_SCRIPT" << REMOTE_BODY
      ACCESS_KEY="$ACCESS_KEY"
      SECRET_KEY="$SECRET_KEY"
      SESSION_TOKEN="$SESSION_TOKEN"
      REMOTE_BODY

      cat >> "$REMOTE_SCRIPT" << 'REMOTE_TAIL'

      echo "--- panel-viz: updating otel-navigator-credentials ---"
      $KUBECTL -n panel-viz create secret generic otel-navigator-credentials \
        --from-literal=AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
        --from-literal=AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        --from-literal=AWS_SESSION_TOKEN="$SESSION_TOKEN" \
        --from-literal=S3_ENDPOINT= \
        --dry-run=client -o yaml | $KUBECTL apply -f -
      $KUBECTL -n panel-viz delete pod -l app=otel-navigator --ignore-not-found
      echo "  panel-viz: secret updated + pod restarted"

      echo ""
      echo "--- dask: patching scheduler and worker env vars ---"
      DEPLOYS=$($KUBECTL -n dask get deploy -o name 2>/dev/null)
      for DEPLOY in $DEPLOYS; do
        $KUBECTL -n dask set env "$DEPLOY" \
          AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
          AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
          AWS_SESSION_TOKEN="$SESSION_TOKEN"
        echo "  patched: $DEPLOY"
      done
      echo "  dask: env vars updated (pods will rolling-restart)"

      echo ""
      echo "--- jupyterhub: helm upgrade with fresh credentials ---"
      cat > /tmp/jupyterhub-creds-override.yaml << EOYAML
      singleuser:
        extraEnv:
          PYTHONPATH: "/srv/jupyterhub-pkg"
          HOME: "/root"
          S3_ENDPOINT: ""
          AWS_ACCESS_KEY_ID: "$ACCESS_KEY"
          AWS_SECRET_ACCESS_KEY: "$SECRET_KEY"
          AWS_SESSION_TOKEN: "$SESSION_TOKEN"
          DASK_SCHEDULER_ADDRESS: "tcp://cybersec-dask-scheduler.dask.svc.cluster.local:8786"
      EOYAML
      $HELM upgrade jupyterhub \
        /home/ec2-user/jupyterhub-4.0.0.tgz \
        -n jupyterhub \
        -f /home/ec2-user/jupyterhub-values.yaml \
        -f /tmp/jupyterhub-creds-override.yaml \
        --timeout 120s
      echo "  jupyterhub: helm upgraded"
      $KUBECTL -n jupyterhub delete pod -l component=singleuser-server --ignore-not-found
      echo "  jupyterhub: singleuser pods deleted (will respawn on login)"

      echo ""
      echo "--- jupyterhub: updating sample-notebooks ConfigMap ---"
      $KUBECTL apply -f /tmp/sample-notebooks-configmap.yaml 2>/dev/null && echo "  configmap updated" || echo "  configmap: skipped (file not found on remote)"

      echo ""
      echo "=== Credential Refresh Complete ==="
      echo ""
      echo "Waiting for panel-viz pod..."
      $KUBECTL -n panel-viz wait --for=condition=Ready pod -l app=otel-navigator --timeout=120s 2>/dev/null || true
      echo ""
      echo "Pod status:"
      echo "panel-viz:"; $KUBECTL -n panel-viz get pods --no-headers
      echo "---"
      echo "dask: $($KUBECTL -n dask get pods --no-headers | wc -l) pods"
      echo "---"
      echo "jupyterhub:"; $KUBECTL -n jupyterhub get pods --no-headers
      REMOTE_TAIL

      # SCP supporting files to control plane
      echo "Uploading files to control plane..."
      eval $SCP_CMD zarf/manifests/jupyterhub-values.yaml ec2-user@$CONTROL_IP:/home/ec2-user/jupyterhub-values.yaml
      eval $SCP_CMD zarf/manifests/sample-notebooks-configmap.yaml ec2-user@$CONTROL_IP:/tmp/sample-notebooks-configmap.yaml
      eval $SCP_CMD "$REMOTE_SCRIPT" ec2-user@$CONTROL_IP:/tmp/refresh-tokens.sh

      # Execute the remote script
      echo "Executing credential refresh on control plane..."
      eval $SSH_CMD ec2-user@$CONTROL_IP "bash /tmp/refresh-tokens.sh"
      rm -f "$REMOTE_SCRIPT"
      echo ""
      echo "Done. S3 access should be restored across all namespaces."
    '';

    # -------------------------------------------------------------------------
    # Dataset Generation
    # -------------------------------------------------------------------------
    # Generates a 1TB+ synthetic OTEL span dataset using shard-partitioned
    # parallel writes for S3 throughput, then deploys the Panel visualization
    # with viewport-aware distributed rasterization.
    #
    # Architecture:
    #   Write: shard=podNN/ prefix per pod eliminates S3 3,500 PUT/s contention
    #   Read:  Hive date/hour partitioning within shards enables partition pruning
    #   File:  Rows sorted by timestamp, 1M row groups for predicate pushdown
    #   Viz:   DynamicMap+RangeXY filters Dask DataFrame BEFORE rasterization
    #
    # Usage:
    #   devenv tasks run aws:generate-dataset              # Full pipeline
    #   devenv tasks run aws:generate-dataset:status       # Monitor progress
    #
    # Cluster requirements: 8+ worker nodes (r6i.xlarge or larger, 32Gi RAM)
    # Time estimate: ~2-4 hours for generation, depends on cluster size
    # Sub-batch streaming: datagen coexists with full-scale Dask (no scale-down needed)

    "aws:generate-dataset".exec = ''
      echo "=== 1TB+ OTEL Dataset Generation Pipeline ==="
      echo ""
      export PROJECT_ROOT="$PWD"

      # Override MinIO credentials with real AWS credentials from profile.
      # Use export-credentials so SSO profiles work too — aws configure get
      # only reads static keys and returns empty for SSO profiles.
      CREDS_JSON=$(aws configure export-credentials --profile "''${AWS_PROFILE:-default}" 2>/dev/null || echo '{}')
      export AWS_ACCESS_KEY_ID=$(echo "$CREDS_JSON" | jq -r '.AccessKeyId // empty')
      export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.SecretAccessKey // empty')
      export AWS_SESSION_TOKEN=$(echo "$CREDS_JSON" | jq -r '.SessionToken // empty')

      if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "❌ Failed to resolve AWS credentials for profile ''${AWS_PROFILE:-default}."
        echo "   For SSO profiles: aws sso login --profile ''${AWS_PROFILE:-default}"
        exit 1
      fi
      unset S3_ENDPOINT

      # Get infrastructure state from tofu
      cd infra/aws/tofu
      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      export AWS_REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')

      if [ -z "$BUCKET_NAME" ]; then
        echo "Error: Could not determine S3 bucket name from Tofu."
        echo "   Run first: devenv tasks run aws:provision"
        exit 1
      fi

      echo "S3 Bucket:  $BUCKET_NAME"
      echo "Region:     $AWS_REGION"

      # Get ingress URLs for display at end
      INGRESS_URLS=$(tofu output -json ingress_urls 2>/dev/null || echo "{}")

      cd ../ansible

      # Step 1: Delete any existing large datagen job (K8s Jobs are immutable)
      echo ""
      echo "--- Step 1/3: Cleaning previous datagen job ---"
      ansible-playbook playbooks/datagen.yml \
        -e '{"datagen_minimal_enabled": false, "datagen_large_enabled": false}' \
        -e "s3_bucket_name=$BUCKET_NAME" \
        -e "aws_region=$AWS_REGION" \
        || true
      # Direct delete via kubectl through ansible
      ansible -m shell -a "sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml delete job datagen-large -n datagen --ignore-not-found" control_plane[0] -b \
        || true

      # Step 2: Deploy shard-partitioned datagen with sub-batch streaming
      # Coexists with full-scale Dask: 8 datagen pods × 4Gi + 32 Dask workers × 4Gi = 160Gi < 256Gi
      echo ""
      echo "--- Step 2/3: Deploying shard-partitioned datagen ---"
      ansible-playbook playbooks/datagen.yml \
        -e '{"datagen_minimal_enabled": false}' \
        -e "s3_bucket_name=$BUCKET_NAME" \
        -e "aws_region=$AWS_REGION"

      # Step 3: Deploy Panel viz with viewport-aware rasterization
      echo ""
      echo "--- Step 3/3: Deploying Panel visualization ---"
      ansible-playbook playbooks/panel-viz.yml \
        -e "s3_bucket_name=$BUCKET_NAME" \
        -e "aws_region=$AWS_REGION"

      echo ""
      echo "=== Dataset Generation Pipeline Started ==="
      echo ""
      echo "Configuration:"
      echo "  Batches:     $(grep datagen_large_batches roles/datagen/defaults/main.yml | awk '{print $2}')"
      echo "  Spans/batch: $(grep datagen_large_spans_per_batch roles/datagen/defaults/main.yml | awk '{print $2}')"
      echo "  Parallelism: $(grep datagen_large_parallelism roles/datagen/defaults/main.yml | awk '{print $2}')"
      echo "  Shard prefix: shard=podNN/date=YYYY-MM-DD/hour=HH/"
      echo "  Target:      s3://$BUCKET_NAME/otel-1t/spans/"
      echo ""
      echo "Monitor progress:"
      echo "  devenv tasks run aws:generate-dataset:status"
      echo ""
      echo "Access URLs (requires WARP):"
      echo "$INGRESS_URLS" | jq -r 'to_entries[] | "  \(.key): \(.value)"' 2>/dev/null || true
    '';

    "aws:generate-dataset:status".exec = ''
      echo "=== Dataset Generation Status ==="
      echo ""

      # Get infrastructure state
      cd infra/aws/tofu
      BUCKET_NAME=$(tofu output -raw s3_bucket_name 2>/dev/null || echo "")
      export AWS_REGION=$(tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      BASTION_IP=$(tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CP_IP=$(tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // ""')
      SSH_KEY="$HOME/.ssh/cybersec-dask.pem"

      if [ -z "$CP_IP" ] || [ -z "$BASTION_IP" ]; then
        echo "Error: Could not get cluster IPs from tofu output"
        exit 1
      fi

      SSH_CMD="ssh -i $SSH_KEY -o ProxyCommand=\"ssh -i $SSH_KEY -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP\" -o StrictHostKeyChecking=no ec2-user@$CP_IP"
      KUBECTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"

      # Job status
      echo "--- Datagen Job ---"
      eval $SSH_CMD "$KUBECTL get job datagen-large -n datagen -o wide 2>&1" || echo "(no job found)"
      echo ""

      # Pod status
      echo "--- Datagen Pods ---"
      eval $SSH_CMD "$KUBECTL get pods -n datagen -o wide 2>&1" || true
      echo ""

      # Sample pod logs (most recent running pod)
      echo "--- Latest Pod Logs (last 15 lines) ---"
      RUNNING_POD=$(eval $SSH_CMD "$KUBECTL get pods -n datagen --field-selector=status.phase=Running -o name 2>/dev/null | head -1" 2>/dev/null || echo "")
      if [ -n "$RUNNING_POD" ]; then
        eval $SSH_CMD "$KUBECTL logs $RUNNING_POD -n datagen --tail=15 2>&1" || true
      else
        echo "(no running pods)"
      fi
      echo ""

      # S3 data size
      echo "--- S3 Dataset Size ---"
      export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile ''${AWS_PROFILE:-default})
      export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile ''${AWS_PROFILE:-default})
      aws s3 ls "s3://$BUCKET_NAME/otel-1t/spans/" --recursive --summarize 2>/dev/null | tail -3 || echo "(no data yet)"
      echo ""

      # Dask cluster status
      echo "--- Dask Workers ---"
      eval $SSH_CMD "$KUBECTL get pods -n dask -l dask.org/component=worker --no-headers 2>&1 | wc -l" 2>/dev/null | xargs -I{} echo "Workers running: {}" || true
      echo ""

      # Panel viz status
      echo "--- Panel Viz ---"
      eval $SSH_CMD "$KUBECTL get pods -n panel-viz -o wide 2>&1" || true
    '';

    "aws:apply".exec = ''
      echo "🔄 Applying configuration changes..."
      echo ""
      echo "This runs all deployment playbooks to apply any changes."
      echo ""

      # Get region from tofu state
      export AWS_REGION=$(cd infra/aws/tofu && tofu output -json cluster_info 2>/dev/null | jq -r '.region // "us-east-1"')
      echo "Using Region: $AWS_REGION"
      echo ""

      cd infra/aws/ansible

      # Run playbooks that are idempotent
      echo "Applying Dask configuration..."
      ansible-playbook playbooks/dask-only.yml -e "aws_region=$AWS_REGION"

      # Check if ngrok credentials are available
      if [ -n "$NGROK_AUTH_TOKEN" ] || [ -n "$NGROK_AUTHTOKEN" ]; then
        echo ""
        echo "Applying ngrok configuration..."
        ansible-playbook playbooks/ngrok.yml -e "aws_region=$AWS_REGION"
      else
        echo ""
        echo "Skipping ngrok (no credentials in environment)"
      fi

      echo ""
      echo "✅ Configuration applied"
    '';

    "aws:verify".exec = ''
      echo "🔍 Verifying AWS cluster services..."
      echo ""

      BASTION_IP=$(cd infra/aws/tofu && tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CONTROL_IP=$(cd infra/aws/tofu && tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty' || echo "")

      if [ -z "$BASTION_IP" ] || [ -z "$CONTROL_IP" ]; then
        echo "❌ Cluster not provisioned. Run 'devenv tasks run aws:provision' first."
        exit 1
      fi

      SSH_CMD="ssh -i ~/.ssh/cybersec-dask.pem -o ProxyCommand=\"ssh -i ~/.ssh/cybersec-dask.pem -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP\" -o StrictHostKeyChecking=no ec2-user@$CONTROL_IP"
      KUBECTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"

      echo "Kubernetes Nodes:"
      eval $SSH_CMD "$KUBECTL get nodes -o wide" 2>/dev/null || echo "  Failed to get nodes"

      echo ""
      echo "Dask Cluster:"
      eval $SSH_CMD "$KUBECTL get daskclusters -A" 2>/dev/null || echo "  No Dask clusters found"

      echo ""
      echo "Ingress Info:"
      INGRESS_PROVIDER=$(cd infra/aws/tofu && tofu output -json ingress_info 2>/dev/null | jq -r '.provider // "unknown"')
      echo "  Provider: $INGRESS_PROVIDER"
      if [ "$INGRESS_PROVIDER" = "cloudflare" ]; then
        echo "  Tunnel status: cloudflared running on bastion"
        eval $SSH_CMD "$KUBECTL get svc -A | grep -E 'dask|jupyter|panel'" 2>/dev/null || echo "  No matching services"
      fi

      echo ""
      echo "External Access Test:"
      for domain in dask.dev.aws.zndx.org jupyter.dev.aws.zndx.org; do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "https://$domain" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "302" ] || [ "$HTTP_CODE" = "200" ]; then
          echo "  $domain: ✅ $HTTP_CODE (OAuth redirect or OK)"
        elif [ "$HTTP_CODE" = "000" ]; then
          echo "  $domain: ❌ Connection failed"
        else
          echo "  $domain: ⚠️  $HTTP_CODE"
        fi
      done
    '';

    "aws:ssh".exec = ''
      BASTION_IP=$(cd infra/aws/tofu && tofu output -raw bastion_public_ip 2>/dev/null || echo "")

      if [ -z "$BASTION_IP" ]; then
        echo "❌ Cluster not provisioned. Run 'devenv tasks run aws:provision' first."
        exit 1
      fi

      TARGET="''${1:-bastion}"

      case "$TARGET" in
        bastion)
          echo "Connecting to bastion ($BASTION_IP)..."
          exec ssh -i ~/.ssh/cybersec-dask.pem \
            -o StrictHostKeyChecking=no ec2-user@$BASTION_IP
          ;;
        control|cp)
          CONTROL_IP=$(cd infra/aws/tofu && tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty')
          echo "Connecting to control plane ($CONTROL_IP) via bastion..."
          exec ssh -i ~/.ssh/cybersec-dask.pem \
            -o ProxyCommand="ssh -i ~/.ssh/cybersec-dask.pem -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP" \
            -o StrictHostKeyChecking=no ec2-user@$CONTROL_IP
          ;;
        *)
          echo "Usage: devenv tasks run aws:ssh [bastion|control|cp]"
          echo ""
          echo "  bastion  - Connect to bastion host (default)"
          echo "  control  - Connect to first control plane node"
          echo "  cp       - Alias for control"
          ;;
      esac
    '';

    "aws:logs:ngrok".exec = ''
      echo "📜 Fetching ngrok operator logs..."

      BASTION_IP=$(cd infra/aws/tofu && tofu output -raw bastion_public_ip 2>/dev/null || echo "")
      CONTROL_IP=$(cd infra/aws/tofu && tofu output -json control_plane_private_ips 2>/dev/null | jq -r '.[0] // empty' || echo "")

      if [ -z "$BASTION_IP" ] || [ -z "$CONTROL_IP" ]; then
        echo "❌ Cluster not provisioned."
        exit 1
      fi

      SSH_CMD="ssh -i ~/.ssh/cybersec-dask.pem -o ProxyCommand=\"ssh -i ~/.ssh/cybersec-dask.pem -W %h:%p -o StrictHostKeyChecking=no ec2-user@$BASTION_IP\" -o StrictHostKeyChecking=no ec2-user@$CONTROL_IP"
      KUBECTL="sudo /var/lib/rancher/rke2/bin/kubectl --kubeconfig /etc/rancher/rke2/rke2.yaml"

      LINES="''${1:-50}"
      eval $SSH_CMD "$KUBECTL logs -n ngrok-system deployment/ngrok-operator-manager --tail=$LINES"
    '';

    # ============================================================================
    # Kubernetes Stack Tasks (on-demand provisioning)
    # ============================================================================

    "k8s:status".exec = ''
      source scripts/lab_env.sh
      echo "Kubernetes Stack Status"
      echo "========================"
      if resolve_kubeconfig; then
        echo "Kubeconfig: $KUBECONFIG"
        echo "Target:     $(detect_k8s_target)"
        if kubectl --kubeconfig="$KUBECONFIG" cluster-info --request-timeout=5s >/dev/null 2>&1; then
          echo "Cluster: Connected"
          kubectl --kubeconfig="$KUBECONFIG" get nodes
          echo ""
          kubectl --kubeconfig="$KUBECONFIG" get pods -A 2>/dev/null | grep -E "dask|jupyter|panel" || echo "No Dask/Jupyter/Panel pods"
        else
          echo "Cluster: Not reachable with this kubeconfig"
        fi
      else
        echo "No usable kubeconfig found"
        echo ""
        echo "To provision k3d:  devenv tasks run k8s:provision"
        echo "To use RKE2:       export KUBECONFIG=~/.kube/rke2.yaml"
        echo "  (or: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$USER ~/.kube/rke2.yaml)"
      fi
    '';

    "k8s:provision".exec = ''
      # Provisions local k3d cluster
      source scripts/polaris_bootstrap_helper.sh

      log_info "Provisioning k3d Kubernetes cluster..."

      # Check podman (macOS)
      if [ "$(uname -s)" = "Darwin" ]; then
        if command -v podman >/dev/null 2>&1; then
          if ! podman machine inspect podman-machine-default >/dev/null 2>&1; then
            log_info "Creating Podman machine..."
            podman machine init --cpus 4 --memory 8192
          fi
          if ! podman machine inspect podman-machine-default 2>/dev/null | grep -q '"Running": true'; then
            log_info "Starting Podman machine..."
            podman machine start podman-machine-default
          fi
        fi
      fi

      # Create k3d cluster
      CLUSTER_NAME="''${K3D_CLUSTER_NAME:-cybersec}"
      if k3d cluster list 2>/dev/null | grep -q "$CLUSTER_NAME"; then
        log_info "Cluster '$CLUSTER_NAME' already exists"
      else
        log_info "Creating k3d cluster '$CLUSTER_NAME'..."
        k3d cluster create "$CLUSTER_NAME" \
          --api-port 6550 \
          --servers 1 \
          --agents 0 \
          --k3s-arg "--disable=traefik@server:0" \
          --k3s-arg "--disable=servicelb@server:0"
      fi

      # Generate kubeconfig
      KCONFIG="$PWD/.devenv/state/kubeconfig"
      mkdir -p "$(dirname "$KCONFIG")"
      k3d kubeconfig get "$CLUSTER_NAME" > "$KCONFIG"
      # Fix API address for localhost access
      sed -i.bak "s|server: .*|server: https://localhost:6550|" "$KCONFIG"
      rm -f "$KCONFIG.bak"

      # Wait for cluster ready
      log_info "Waiting for cluster to be ready..."
      kubectl --kubeconfig="$KCONFIG" wait --for=condition=Ready nodes --all --timeout=120s

      log_success "k3d cluster provisioned"
      echo ""
      echo "KUBECONFIG=$KCONFIG"
      echo ""
      echo "Next: devenv tasks run k8s:deploy-dask"
    '';

    "k8s:deploy-dask".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      KCONFIG="''${KUBECONFIG:-$PWD/.devenv/state/kubeconfig}"

      if [ ! -f "$KCONFIG" ]; then
        log_error "No kubeconfig found. Run: devenv tasks run k8s:provision"
        exit 1
      fi
      export KUBECONFIG="$KCONFIG"

      log_info "Deploying Dask operator..."
      helm repo add dask https://helm.dask.org || true
      helm repo update dask
      helm upgrade --install dask-operator dask/dask-kubernetes-operator \
        --namespace dask-operator --create-namespace \
        --wait --timeout 5m

      log_info "Waiting for CRDs..."
      kubectl wait --for=condition=Established crd/daskclusters.kubernetes.dask.org --timeout=60s

      log_info "Deploying Dask cluster..."
      kubectl apply -f infra/dask/dask-cluster.yaml

      log_info "Waiting for Dask cluster to be Running..."
      for i in $(seq 1 60); do
        PHASE=$(kubectl get daskcluster -n dask cybersec-dask -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [ "$PHASE" = "Running" ]; then
          log_success "Dask cluster is Running"
          break
        fi
        sleep 5
      done

      echo ""
      echo "Start port-forward: devenv tasks run k8s:forward"
    '';

    "k8s:deploy-jupyter".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      KCONFIG="''${KUBECONFIG:-$PWD/.devenv/state/kubeconfig}"

      if [ ! -f "$KCONFIG" ]; then
        log_error "No kubeconfig found. Run: devenv tasks run k8s:provision"
        exit 1
      fi
      export KUBECONFIG="$KCONFIG"

      log_info "Deploying JupyterHub..."
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ || true
      helm repo update jupyterhub
      helm upgrade --install jupyterhub jupyterhub/jupyterhub \
        --namespace jupyterhub --create-namespace \
        --version 2.0.0 \
        --set singleuser.cpu.limit=0.5 \
        --set singleuser.memory.limit=512Mi \
        --wait --timeout 10m

      log_success "JupyterHub deployed"
      echo ""
      echo "Start port-forward: devenv tasks run k8s:forward"
    '';

    "k8s:forward".exec = ''
      KCONFIG="''${KUBECONFIG:-$PWD/.devenv/state/kubeconfig}"

      if [ ! -f "$KCONFIG" ]; then
        echo "No kubeconfig found. Run: devenv tasks run k8s:provision"
        exit 1
      fi
      export KUBECONFIG="$KCONFIG"

      echo "Starting port-forwards (Ctrl+C to stop)..."
      echo "  Dask Dashboard:  http://localhost:8787"
      echo "  JupyterHub:      http://localhost:8000"
      echo ""

      # Run port-forwards in parallel
      kubectl port-forward -n dask svc/cybersec-dask-scheduler 8787:8787 &
      PF1=$!
      kubectl port-forward -n jupyterhub svc/proxy-public 8000:80 &
      PF2=$!

      trap "kill $PF1 $PF2 2>/dev/null" EXIT
      wait
    '';

    "k8s:destroy".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      log_warn "Destroying k3d cluster..."

      CLUSTER_NAME="''${K3D_CLUSTER_NAME:-cybersec}"
      k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true
      rm -f "$PWD/.devenv/state/kubeconfig"

      log_success "k3d cluster destroyed"
    '';

    # =========================================================================
    # K8s Target Preparation Tasks
    # =========================================================================
    # These tasks validate and configure K8s targets using conftest policies.
    # Each target has specific requirements validated before deployment.

    "k8s:prepare".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      log_info "=== K8s Target Preparation ==="

      # Auto-detect target from environment
      if [ -n "''${CYBERSEC_K8S_TARGET:-}" ]; then
        TARGET="$CYBERSEC_K8S_TARGET"
      elif [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
        if grep -q "rke2\|rancher" "$KUBECONFIG" 2>/dev/null; then
          TARGET="rke2"
        elif grep -q "k3d\|k3s" "$KUBECONFIG" 2>/dev/null; then
          TARGET="k3d"
        else
          TARGET=""
        fi
      elif [ -n "''${AWS_ACCESS_KEY_ID:-}" ] && [ -n "''${NGROK_AUTH_TOKEN:-}" ]; then
        TARGET="aws"
      else
        TARGET=""
      fi

      if [ -z "$TARGET" ]; then
        log_info "No target auto-detected. Available targets:"
        echo "  devenv tasks run k8s:prepare-aws   # AWS RKE2 with Dask/JupyterHub"
        echo "  devenv tasks run k8s:prepare-rke2  # Local RKE2 cluster"
        echo "  devenv tasks run k8s:prepare-k3d   # Local k3d development"
        echo ""
        echo "Set CYBERSEC_K8S_TARGET or KUBECONFIG to auto-detect."
        exit 0
      fi

      log_info "Detected target: $TARGET"
      case "$TARGET" in
        aws)  devenv tasks run k8s:prepare-aws ;;
        rke2) devenv tasks run k8s:prepare-rke2 ;;
        k3d)  devenv tasks run k8s:prepare-k3d ;;
        *)
          log_error "Unknown target: $TARGET"
          exit 1
          ;;
      esac
    '';

    "k8s:prepare-aws".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      log_info "=== AWS K8s Target Preparation ==="
      log_info "Validating AWS deployment requirements..."

      # Ensure local kubeconfig doesn't cause AWS validation failures
      unset KUBECONFIG
      export CYBERSEC_K8S_TARGET=aws

      # Generate environment config for conftest
      uv run python -c "
import asyncio
import json
from pathlib import Path
from cybersec.config.runtime import gather_runtime_config

async def main():
    config = await gather_runtime_config()
    # Flatten for policy consumption
    env = {
        'platform': config.get('platform', {}),
        'tools': config.get('tools', {}),
        'kubernetes': config.get('kubernetes', {}),
        'aws': config.get('aws', {}),
        'services': config.get('services', {}),
    'developer': config.get('developer', {}),
    }
    Path('build').mkdir(exist_ok=True)
    Path('build/environment.json').write_text(json.dumps(env, indent=2))
    print('Environment config written to build/environment.json')

asyncio.run(main())
"

      # Run conftest validation
      log_info "Running policy validation..."
      if conftest test build/environment.json \
          --policy policy/k8s/base.rego \
          --policy policy/k8s/aws/ \
          --all-namespaces; then
        log_success "Validation passed!"

        # Generate target config
        log_info "Generating target configuration..."
        uv run python -c "
import asyncio
from cybersec.k8s.prepare import prepare_target, K8sTarget

async def main():
    result = await prepare_target(K8sTarget.AWS, dry_run=False)
    if result.success:
        print(f'Config written to: {result.config_path}')
    else:
        print(f'Failed: {result.message}')
        for deny in result.validation.denies:
            print(f'  - {deny}')
        exit(1)

asyncio.run(main())
"
        log_success "AWS target prepared. Run: devenv tasks run aws:provision"
      else
        log_error "Validation failed. Fix the issues above and retry."
        exit 1
      fi
    '';

    "k8s:prepare-rke2".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      log_info "=== RKE2 K8s Target Preparation ==="
      log_info "Validating RKE2 deployment requirements..."

      # Ensure KUBECONFIG is set
      if [ -z "''${KUBECONFIG:-}" ]; then
        log_warn "KUBECONFIG not set. Using default ~/.kube/rke2.yaml"
        export KUBECONFIG="$HOME/.kube/rke2.yaml"
      fi

      # Generate environment config for conftest
      uv run python -c "
import asyncio
import json
import os
from pathlib import Path
from cybersec.config.runtime import gather_runtime_config

os.environ.setdefault('CYBERSEC_K8S_TARGET', 'rke2')

async def main():
    config = await gather_runtime_config()
    env = {
        'platform': config.get('platform', {}),
        'tools': config.get('tools', {}),
        'kubernetes': config.get('kubernetes', {}),
        'services': config.get('services', {}),
    }
    Path('build').mkdir(exist_ok=True)
    Path('build/environment.json').write_text(json.dumps(env, indent=2))
    print('Environment config written to build/environment.json')

asyncio.run(main())
"

      # Run conftest validation
      log_info "Running policy validation..."
      if conftest test build/environment.json \
          --policy policy/k8s/base.rego \
          --policy policy/k8s/rke2/ \
          --all-namespaces; then
        log_success "Validation passed!"

        # Generate target config
        log_info "Generating target configuration..."
        uv run python -c "
import asyncio
from cybersec.k8s.prepare import prepare_target, K8sTarget

async def main():
    result = await prepare_target(K8sTarget.RKE2, dry_run=False)
    if result.success:
        print(f'Config written to: {result.config_path}')
    else:
        print(f'Failed: {result.message}')
        exit(1)

asyncio.run(main())
"
        log_success "RKE2 target prepared. Run: devenv tasks run k8s:deploy-dask"
      else
        log_error "Validation failed. Fix the issues above and retry."
        exit 1
      fi
    '';

    "k8s:prepare-k3d".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      log_info "=== k3d K8s Target Preparation ==="
      log_info "Validating k3d deployment requirements..."

      # Generate environment config for conftest
      uv run python -c "
import asyncio
import json
import os
from pathlib import Path
from cybersec.config.runtime import gather_runtime_config

os.environ.setdefault('CYBERSEC_K8S_TARGET', 'k3d')

async def main():
    config = await gather_runtime_config()
    env = {
        'platform': config.get('platform', {}),
        'tools': config.get('tools', {}),
        'kubernetes': config.get('kubernetes', {}),
        'services': config.get('services', {}),
    }
    Path('build').mkdir(exist_ok=True)
    Path('build/environment.json').write_text(json.dumps(env, indent=2))
    print('Environment config written to build/environment.json')

asyncio.run(main())
"

      # Run conftest validation
      log_info "Running policy validation..."
      if conftest test build/environment.json \
          --policy policy/k8s/base.rego \
          --policy policy/k8s/k3d/ \
          --all-namespaces; then
        log_success "Validation passed!"

        # Generate target config
        log_info "Generating target configuration..."
        uv run python -c "
import asyncio
from cybersec.k8s.prepare import prepare_target, K8sTarget

async def main():
    result = await prepare_target(K8sTarget.K3D, dry_run=False)
    if result.success:
        print(f'Config written to: {result.config_path}')
    else:
        print(f'Failed: {result.message}')
        exit(1)

asyncio.run(main())
"
        log_success "k3d target prepared. Run: devenv tasks run k8s:provision"
      else
        log_error "Validation failed. Fix the issues above and retry."
        exit 1
      fi
    '';

    # ============================================================================
    # Zarf Air-Gap Deployment Tasks
    # ============================================================================
    # These tasks manage Zarf package creation and deployment for air-gap environments.
    # Target: RKE2 clusters without internet access.

    "zarf:preflight".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Zarf Air-Gap Preflight Validation ==="
      uv run cyberphy "/zarf preflight"
    '';

    "zarf:image".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Building Cyberphy Dask Image ==="

      # Prefer podman, fallback to docker
      if command -v podman &>/dev/null; then
        BUILDER="podman"
      elif command -v docker &>/dev/null; then
        BUILDER="docker"
      else
        log_error "No container runtime found. Install podman or docker."
        exit 1
      fi

      log_info "Using $BUILDER to build image..."

      # zarf.yaml declares architecture: amd64 and the deployment target is
      # x86_64 EC2 (m6i/r6i). On Apple Silicon hosts we must force linux/amd64
      # via Rosetta — building for the host arch produces an arm64 image with
      # libstdc++/wheel incompatibilities and an undeployable artifact.
      # Build BOTH the plain tag AND the localhost:5555/ tag that zarf.yaml +
      # the manifests reference, so `zarf package create` bundles THIS build.
      # Previously only cybersec-dask:<tag> was built, leaving the localhost:5555/
      # tag stale → the package silently shipped the OLD image (2026-06-08 redeploy).
      $BUILDER build \
        --platform linux/amd64 \
        -t cybersec-dask:2025.2.1 \
        -t localhost:5555/cybersec-dask:2025.2.1 \
        -f zarf/images/Dockerfile.cybersec-dask \
        --build-arg BASE_IMAGE=ghcr.io/dask/dask:2025.2.0 \
        .

      if [ $? -eq 0 ]; then
        log_success "Image built: cybersec-dask:2025.2.1 (+ localhost:5555/ tag)"
        echo ""
        echo "Next: devenv tasks run zarf:package"
      else
        log_error "Image build failed"
        exit 1
      fi
    '';

    "zarf:package".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Creating Zarf Package ==="

      # Check if custom image exists
      if command -v podman &>/dev/null; then
        IMG=$(podman images -q cybersec-dask:2025.2.1 2>/dev/null)
      elif command -v docker &>/dev/null; then
        IMG=$(docker images -q cybersec-dask:2025.2.1 2>/dev/null)
      fi

      if [ -z "$IMG" ]; then
        log_error "Custom image not found. Run: devenv tasks run zarf:image"
        exit 1
      fi

      # sample-notebooks ConfigMap is a packaged file — re-embed so HDF5 +
      # generate_hdf5.py + cluster_env.py land in JupyterHub in situ.
      log_info "Embedding/verifying sample notebooks ConfigMap..."
      python3 zarf/scripts/verify-sample-notebooks.py || {
        log_error "sample notebooks gate failed — fix zarf/notebooks/ then retry"
        exit 1
      }

      cd zarf
      log_info "Running zarf package create..."
      zarf package create --confirm

      if [ $? -eq 0 ]; then
        PKG=$(ls -t zarf-package-cybersec-dask-*.tar.zst 2>/dev/null | head -1)
        log_success "Package created: $PKG"

        # Closure gate: every Layer-A image declared in artifacts.manifest.json
        # MUST be in the bundle, and the bundle MUST stay under the GitHub
        # release-asset size budget. Catches "we forgot to package X" at BUILD
        # time instead of stranding the air-gap cluster at deploy time.
        log_info "Running closure gate (artifacts.manifest.json)..."
        if ! python3 scripts/check-closure.py "$PKG"; then
          log_error "Closure gate FAILED — package incomplete or over budget (see above)."
          exit 1
        fi

        # Build the Layer-A bootstrap-images tarball (local-path-provisioner +
        # busybox) for air-gap node preload (task #4): RKE2 imports it from
        # agent/images/ at start so the StorageClass provisioner + helper pod run
        # with NO registry and NO egress. The ansible `common` role copies it onto
        # every node before rke2 starts.
        log_info "Building bootstrap-images tarball for node preload..."
        if ! bash scripts/build-bootstrap-images.sh; then
          log_error "Failed to build bootstrap-images tarball (need podman/docker)."
          exit 1
        fi

        echo ""
        echo "Transfer package to air-gap environment and deploy with:"
        echo "  zarf init --confirm"
        echo "  zarf package deploy $PKG --confirm"
      else
        log_error "Package creation failed"
        exit 1
      fi
    '';

    "zarf:deploy".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Deploying Zarf Package ==="

      cd zarf
      PKG=$(ls -t zarf-package-cybersec-dask-*.tar.zst 2>/dev/null | head -1)

      if [ -z "$PKG" ]; then
        log_error "No Zarf package found. Run: devenv tasks run zarf:package"
        exit 1
      fi

      log_info "Deploying: $PKG"
      zarf package deploy "$PKG" --confirm

      if [ $? -eq 0 ]; then
        log_success "Package deployed successfully!"
        echo ""
        echo "Verify deployment:"
        echo "  kubectl get pods -n dask"
        echo "  kubectl get pods -n jupyterhub"
        echo "  kubectl get pods -n panel-viz"
      else
        log_error "Deployment failed"
        exit 1
      fi
    '';

    "zarf:status".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Zarf Deployment Status ==="

      # Check for Zarf init
      if kubectl get ns zarf &>/dev/null; then
        log_success "Zarf initialized (zarf namespace exists)"
      else
        log_warn "Zarf not initialized. Run: zarf init --confirm"
      fi

      echo ""
      echo "Dask namespace:"
      kubectl get pods -n dask 2>/dev/null || echo "  (namespace not found)"

      echo ""
      echo "JupyterHub namespace:"
      kubectl get pods -n jupyterhub 2>/dev/null || echo "  (namespace not found)"

      echo ""
      echo "Panel-Viz namespace:"
      kubectl get pods -n panel-viz 2>/dev/null || echo "  (namespace not found)"

      echo ""
      echo "Services:"
      echo "  Dask Dashboard: http://<node-ip>:30087"
      echo "  JupyterHub:     http://<node-ip>:30080"
      echo "  Panel-Viz:      http://<node-ip>:30506"
    '';

    "zarf:datagen".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== OTEL Synthetic Data Generator ==="

      MODE="''${1:-minimal}"
      S3_BUCKET="''${S3_BUCKET:-$OTEL_S3_BUCKET}"
      S3_BUCKET="''${S3_BUCKET:-cybersec-dask-data}"

      if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        log_error "AWS credentials not set"
        log_info "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or use:"
        log_info "  export AWS_PROFILE=your-profile && eval \$(aws configure export-credentials --format env)"
        exit 1
      fi

      log_info "Mode: $MODE"
      log_info "Bucket: $S3_BUCKET"

      uv run python zarf/scripts/generate-otel-data.py \
        --mode "$MODE" \
        --bucket "$S3_BUCKET" \
        --region "''${AWS_REGION:-us-east-1}" \
        ''${S3_ENDPOINT:+--endpoint "$S3_ENDPOINT"}
    '';

    # ========================================================================
    # Local Zarf Deployment Tasks
    #
    # Deploy Dask+JupyterHub+Panel-Viz to a local RKE2 or k3d cluster
    # running alongside the Flink devenv stack. Uses disk-light defaults
    # (no registry PVC, emptyDir spill).
    # ========================================================================

    "zarf:local:preflight".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      source scripts/lab_env.sh
      log_info "=== Local Zarf Deployment Preflight ==="

      resolve_kubeconfig || { log_error "No usable kubeconfig (try ~/.kube/rke2.yaml)"; exit 1; }
      log_info "KUBECONFIG=$KUBECONFIG target=$(detect_k8s_target)"
      ensure_local_s3_env

      # --- Quick local S3 (RustFS) check ---
      if ! rustfs_up "http://localhost:''${LOCAL_S3_PORT:-9010}"; then
        log_warn "Local S3 (RustFS) not running on :''${LOCAL_S3_PORT:-9010}. Required: devenv up -d"
      else
        log_success "RustFS healthy on :''${LOCAL_S3_PORT:-9010}"
      fi

      # --- Gather config and run conftest ---
      log_info "Gathering runtime configuration..."
      uv run python -c "
import asyncio, json
from cybersec.zarf.local import gather_local_zarf_config
config = asyncio.run(gather_local_zarf_config())
from pathlib import Path
Path('build').mkdir(exist_ok=True)
Path('build/environment.json').write_text(json.dumps(config, indent=2))
print('Config written to build/environment.json')
      "

      if [ $? -ne 0 ]; then
        log_error "Failed to gather runtime config"
        exit 1
      fi

      log_info "Running conftest policy validation..."
      conftest test build/environment.json \
        --policy policy/k8s/base.rego \
        --policy policy/k8s/local/ \
        --all-namespaces

      RESULT=$?
      echo ""
      if [ $RESULT -eq 0 ]; then
        log_success "Preflight PASSED"
        echo ""
        echo "Next: devenv tasks run zarf:local:init"
      else
        log_error "Preflight FAILED — resolve errors above before proceeding"
        exit 1
      fi
    '';

    "zarf:local:init".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      source scripts/lab_env.sh
      log_info "=== Local Zarf Init (disk-light) ==="

      resolve_kubeconfig || {
        log_error "No usable kubeconfig. Fix: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$(id -u):\$(id -g) ~/.kube/rke2.yaml"
        exit 1
      }
      log_info "KUBECONFIG=$KUBECONFIG target=$(detect_k8s_target)"

      # --- Check if already initialized ---
      if kubectl get ns zarf &>/dev/null; then
        if kubectl get deploy -n zarf zarf-docker-registry &>/dev/null; then
          log_success "Zarf already initialized (registry running). Skipping init."
          echo ""
          echo "Next: devenv tasks run zarf:local:deploy"
          exit 0
        fi
      fi

      # --- Find or download init package ---
      cd zarf
      INIT_PKG=$(ls -t zarf-init-*.tar.zst 2>/dev/null | head -1)

      if [ -z "$INIT_PKG" ]; then
        if [ -n "''${AIRGAP:-}" ]; then
          log_error "No init package found in zarf/ and AIRGAP mode is set."
          log_error "Pre-download in a connected environment: zarf tools download-init"
          exit 1
        fi
        log_info "No init package found in zarf/. Downloading..."
        ZARF_VERSION=$(zarf version 2>/dev/null || echo "v0.41.0")
        zarf tools download-init
        INIT_PKG=$(ls -t zarf-init-*.tar.zst 2>/dev/null | head -1)
        if [ -z "$INIT_PKG" ]; then
          log_error "Failed to download init package"
          exit 1
        fi
      fi

      log_info "Using init package: $INIT_PKG"

      # Check if a default StorageClass exists
      DEFAULT_SC=$(kubectl get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}' 2>/dev/null)

      if [ -n "$DEFAULT_SC" ]; then
        log_info "StorageClass '$DEFAULT_SC' available — using small PVC for registry"
        zarf init --confirm --set REGISTRY_PVC_SIZE=1Gi
      else
        log_info "No StorageClass — installing local-path-provisioner from vendored manifest"
        kubectl apply -f zarf/manifests/local-path-provisioner.yaml 2>/dev/null || true
        kubectl wait --for=condition=ready pod -l app=local-path-provisioner -n local-path-storage --timeout=60s 2>/dev/null || true
        log_success "local-path StorageClass installed and set as default"
        zarf init --confirm --set REGISTRY_PVC_SIZE=1Gi
      fi

      if [ $? -eq 0 ]; then
        log_success "Zarf initialized successfully"
        echo ""
        echo "Next: devenv tasks run zarf:local:deploy"
      else
        log_error "Zarf init failed"
        exit 1
      fi
    '';

    "zarf:local:deploy".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      source scripts/lab_env.sh
      log_info "=== Local Zarf Deploy ==="

      resolve_kubeconfig || { log_error "No usable kubeconfig"; exit 1; }
      log_info "KUBECONFIG=$KUBECONFIG target=$(detect_k8s_target)"
      ensure_local_s3_env

      # --- Verify Zarf is initialized ---
      if ! kubectl get ns zarf &>/dev/null; then
        log_error "Zarf not initialized. Run: devenv tasks run zarf:local:init"
        exit 1
      fi

      # --- Find deploy package (prefer mirror 1.6.5+) ---
      PKG=$(find_zarf_package) || true
      if [ -z "$PKG" ]; then
        log_error "No deploy package found (build/cyberphy-release-mirror or zarf/)"
        exit 1
      fi
      # zarf CLI wants CWD-relative or absolute path
      PKG=$(readlink -f "$PKG")

      # Multi-core lab default 4; override DASK_WORKER_REPLICAS=1 for tiny hosts
      WORKERS="''${DASK_WORKER_REPLICAS:-4}"
      SPILL_DIR="''${DASK_SPILL_DIR:-}"
      S3_PORT="''${LOCAL_S3_PORT:-9010}"

      # --- Detect local S3 (RustFS) endpoint for K8s pods ---
      NODE_IP=$(detect_node_ip)
      if [ -z "$NODE_IP" ] || [ "$NODE_IP" = "127.0.0.1" ]; then
        NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' | awk '{print $1}')
      fi
      if [ -z "$NODE_IP" ]; then
        log_error "Cannot detect node InternalIP. Is the cluster running?"
        exit 1
      fi

      if ! rustfs_up "http://''${NODE_IP}:''${S3_PORT}"; then
        if rustfs_up "http://localhost:''${S3_PORT}"; then
          log_warn "RustFS reachable at localhost but not at ''${NODE_IP}:''${S3_PORT} — pods may fail"
        else
          log_error "RustFS not running. Start with: devenv up -d"
          exit 1
        fi
      fi

      S3_EP="http://''${NODE_IP}:''${S3_PORT}"
      S3_AK="''${RUSTFS_ACCESS_KEY:-''${MINIO_ACCESS_KEY:-admin}}"
      S3_SK="''${RUSTFS_SECRET_KEY:-''${MINIO_SECRET_KEY:-admin}}"
      S3_BK="''${S3_BUCKET:-cyberphy}"
      S3_RG="''${S3_REGION:-us-east-1}"

      log_info "Deploying: $PKG"
      log_info "Workers: $WORKERS"
      log_info "Spill dir: ''${SPILL_DIR:-emptyDir (disk-light)}"
      log_info "S3 endpoint: $S3_EP (RustFS on node $NODE_IP) bucket=$S3_BK"

      # Secrets via env (ZARF_VAR_*) when supported; also pass --set for non-secrets
      export ZARF_VAR_S3_ACCESS_KEY="$S3_AK"
      export ZARF_VAR_S3_SECRET_KEY="$S3_SK"

      zarf package deploy "$PKG" --confirm \
        --set DASK_WORKER_REPLICAS="$WORKERS" \
        --set S3_ENDPOINT="$S3_EP" \
        --set S3_BUCKET="$S3_BK" \
        --set S3_REGION="$S3_RG" \
        --set S3_ACCESS_KEY="$S3_AK" \
        --set S3_SECRET_KEY="$S3_SK" \
        ''${SPILL_DIR:+--set DASK_SPILL_DIR="$SPILL_DIR"}

      if [ $? -ne 0 ]; then
        log_error "Zarf deploy failed"
        exit 1
      fi

      # --- Post-deploy spill patch ---
      # If DASK_SPILL_DIR is empty or path doesn't exist, patch to emptyDir
      if [ -z "$SPILL_DIR" ] || [ ! -d "$SPILL_DIR" ]; then
        log_info "Patching Dask workers for emptyDir spill (disk-light)..."

        # Check if daskcluster exists before patching
        if kubectl get daskcluster cybersec-dask -n dask &>/dev/null; then
          kubectl patch daskcluster cybersec-dask -n dask --type=json \
            -p '[{"op":"replace","path":"/spec/worker/spec/volumes/0","value":{"name":"dask-spill","emptyDir":{"sizeLimit":"512Mi"}}}]' \
            2>/dev/null || log_warn "Could not patch daskcluster spill volume (may not have volume at index 0)"

          # Restart workers to pick up the change
          kubectl rollout restart deployment -n dask -l dask.org/component=worker 2>/dev/null \
            || kubectl delete pods -n dask -l dask.org/component=worker 2>/dev/null \
            || true
          log_info "Workers restarting with emptyDir spill"
        fi
      else
        log_info "Spill dir exists at $SPILL_DIR — keeping hostPath volume"
      fi

      # --- Wait for Panel-Viz ---
      log_info "Waiting for Panel-Viz pod to be ready (120s timeout)..."
      if kubectl wait --for=condition=ready pod -l app=otel-navigator -n panel-viz --timeout=120s 2>/dev/null; then
        PANEL_READY=0
      else
        PANEL_READY=1
      fi

      # --- Check Panel-Viz accessibility ---
      PANEL_PORT="''${PANEL_PORT:-30506}"
      PANEL_BIND="''${PANEL_BIND_ADDRESS:-0.0.0.0}"

      if [ $PANEL_READY -eq 0 ]; then
        # Try NodePort first (RKE2 usually exposes NodePorts directly)
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$PANEL_PORT/" 2>/dev/null || echo "000")
        if echo "$HTTP_CODE" | grep -q "200\|301\|302"; then
          log_success "Panel-Viz accessible via NodePort $PANEL_PORT (HTTP $HTTP_CODE)"
        else
          log_info "NodePort not directly accessible (HTTP $HTTP_CODE). Starting port-forward..."
          kubectl port-forward -n panel-viz svc/otel-navigator "$PANEL_PORT:5006" --address="$PANEL_BIND" &
          PF_PID=$!
          sleep 2
          if kill -0 $PF_PID 2>/dev/null; then
            log_success "Port-forward started (PID $PF_PID)"
          else
            log_warn "Port-forward may have failed. Check manually."
          fi
        fi
      else
        log_warn "Panel-Viz not ready within 120s. Check: kubectl get pods -n panel-viz"
      fi

      # --- Service URLs banner ---
      echo ""
      log_success "=== Deployment Complete ==="
      echo ""
      echo "Service URLs:"
      echo "  Panel-Viz:      http://$PANEL_BIND:$PANEL_PORT/"
      echo "  Dask Dashboard: http://localhost:30087/"
      echo "  JupyterHub:     http://localhost:30080/"
      echo ""
      echo "Status:  devenv tasks run zarf:local:status"
      echo "Logs:    kubectl logs -n panel-viz -l app=otel-navigator -f"
    '';

    # ===========================================================================
    # K8s Dashboard — separate Zarf package (keeps main package under size limit)
    # ===========================================================================

    "zarf:local:package-dashboard".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Creating K8s Dashboard Zarf Package ==="

      cd zarf/kubernetes-dashboard
      zarf package create --confirm

      PKG=$(ls -t zarf-package-cybersec-k8s-dashboard-*.tar.zst 2>/dev/null | head -1)
      if [ -n "$PKG" ]; then
        log_success "Package created: $PKG"
      else
        log_error "Package creation failed"
        exit 1
      fi
    '';

    "zarf:local:deploy-dashboard".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      source scripts/lab_env.sh
      log_info "=== Deploying K8s Dashboard ==="

      resolve_kubeconfig || {
        log_error "No usable kubeconfig. Fix: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$(id -u):\$(id -g) ~/.kube/rke2.yaml"
        exit 1
      }
      log_info "KUBECONFIG=$KUBECONFIG target=$(detect_k8s_target)"

      # --- Verify Zarf is initialized ---
      if ! kubectl get ns zarf &>/dev/null; then
        log_error "Zarf not initialized. Run: devenv tasks run zarf:local:init"
        exit 1
      fi

      # --- Find or create package ---
      cd zarf/kubernetes-dashboard
      PKG=$(ls -t zarf-package-cybersec-k8s-dashboard-*.tar.zst 2>/dev/null | head -1)
      if [ -z "$PKG" ]; then
        log_info "No dashboard package found — building..."
        zarf package create --confirm
        PKG=$(ls -t zarf-package-cybersec-k8s-dashboard-*.tar.zst 2>/dev/null | head -1)
        if [ -z "$PKG" ]; then
          log_error "Package creation failed"
          exit 1
        fi
      fi

      log_info "Deploying: $PKG"
      if ! zarf package deploy "$PKG" --confirm; then
        log_error "Dashboard deploy failed"
        exit 1
      fi

      # --- Wait for dashboard ---
      log_info "Waiting for dashboard pod..."
      kubectl -n kubernetes-dashboard rollout status deploy/kubernetes-dashboard --timeout=120s || true

      # --- Kick process-compose port-forward if available (devenv up) ---
      if process-compose process restart k8s-dashboard 2>/dev/null; then
        log_info "Port-forward started via process-compose"
      fi

      echo ""
      log_success "=== K8s Dashboard Deployed ==="
      echo ""
      echo "  Access (pick one):"
      echo "    zarf connect kubernetes-dashboard          # Zarf-native (works anywhere)"
      echo "    https://localhost:10443                    # devenv up (auto port-forward)"
      echo ""
      echo "  Token:"
      echo "    kubectl -n kubernetes-dashboard create token admin-user"
      echo "    Or generate from Settings page: http://localhost:5050/settings"
    '';

    "zarf:local:status".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      source scripts/lab_env.sh
      log_info "=== Local Zarf Deployment Status ==="

      resolve_kubeconfig || {
        log_error "No usable kubeconfig. Fix: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$(id -u):\$(id -g) ~/.kube/rke2.yaml"
        exit 1
      }
      ensure_local_s3_env
      echo "KUBECONFIG: $KUBECONFIG  target=$(detect_k8s_target)"
      echo ""

      # --- Local S3 (RustFS) ---
      S3_PORT="''${LOCAL_S3_PORT:-9010}"
      echo "Local S3 (RustFS):"
      if rustfs_up "http://localhost:''${S3_PORT}"; then
        log_success "  http://localhost:''${S3_PORT}/ (healthy)"
        NODE_IP=$(detect_node_ip)
        echo "  Pod endpoint: http://''${NODE_IP}:''${S3_PORT}"
        echo "  Bucket: ''${S3_BUCKET:-cyberphy}  Credentials: ''${RUSTFS_ACCESS_KEY:-admin}/''${RUSTFS_SECRET_KEY:-admin}"
      else
        log_warn "  http://localhost:''${S3_PORT}/ (not running — devenv up -d)"
      fi
      if pkg=$(find_zarf_package 2>/dev/null); then
        echo "  Package: $pkg"
      fi
      echo ""

      # --- Zarf namespace ---
      echo "Zarf:"
      if kubectl get ns zarf &>/dev/null; then
        kubectl get pods -n zarf --no-headers 2>/dev/null | sed 's/^/  /'
      else
        echo "  (not initialized)"
      fi

      # --- Dask operator ---
      echo ""
      echo "Dask Operator:"
      if kubectl get ns dask-operator &>/dev/null; then
        kubectl get pods -n dask-operator --no-headers 2>/dev/null | sed 's/^/  /'
      else
        echo "  (namespace not found)"
      fi

      # --- Dask cluster ---
      echo ""
      echo "Dask:"
      if kubectl get ns dask &>/dev/null; then
        kubectl get pods -n dask --no-headers 2>/dev/null | sed 's/^/  /'
        echo ""
        echo "  DaskCluster:"
        kubectl get daskcluster -n dask --no-headers 2>/dev/null | sed 's/^/    /' || echo "    (none)"
      else
        echo "  (namespace not found)"
      fi

      # --- JupyterHub ---
      echo ""
      echo "JupyterHub:"
      if kubectl get ns jupyterhub &>/dev/null; then
        kubectl get pods -n jupyterhub --no-headers 2>/dev/null | sed 's/^/  /'
      else
        echo "  (namespace not found)"
      fi

      # --- Panel-Viz ---
      echo ""
      echo "Panel-Viz:"
      if kubectl get ns panel-viz &>/dev/null; then
        kubectl get pods -n panel-viz --no-headers 2>/dev/null | sed 's/^/  /'
      else
        echo "  (namespace not found)"
      fi

      # --- Service accessibility (NodePorts) ---
      echo ""
      echo "Service Accessibility:"
      PANEL_PORT="''${PANEL_PORT:-30506}"

      if nodeport_up "$PANEL_PORT" /; then
        log_success "  Panel-Viz:      http://localhost:$PANEL_PORT/"
      else
        log_warn "  Panel-Viz:      http://localhost:$PANEL_PORT/ (not accessible)"
      fi

      if nodeport_up 30087 /health || nodeport_up 30087 /; then
        log_success "  Dask Dashboard: http://localhost:30087/"
      else
        log_warn "  Dask Dashboard: http://localhost:30087/ (not accessible)"
      fi

      if nodeport_up 30080 /hub/login || nodeport_up 30080 /; then
        log_success "  JupyterHub:     http://localhost:30080/"
      else
        log_warn "  JupyterHub:     http://localhost:30080/ (not accessible)"
      fi

      # Local S3 / RustFS (pod-reachable via node IP)
      NODE_IP=$(detect_node_ip)
      if [ -n "$NODE_IP" ] && [ "$NODE_IP" != "127.0.0.1" ]; then
        if rustfs_up "http://''${NODE_IP}:''${S3_PORT}"; then
          log_success "  RustFS (pods):  http://''${NODE_IP}:''${S3_PORT}/ (reachable)"
        else
          log_warn "  RustFS (pods):  http://''${NODE_IP}:''${S3_PORT}/ (not reachable from node)"
        fi
      fi

      # --- Node resources ---
      echo ""
      echo "Node Resources:"
      kubectl top nodes 2>/dev/null | sed 's/^/  /' || echo "  (metrics-server not available)"
    '';

    "zarf:local:reset".exec = ''
      source scripts/polaris_bootstrap_helper.sh
      log_info "=== Local Zarf Reset (full teardown for E2E testing) ==="

      # --- Detect kubeconfig (with permission-aware fallback) ---
      _resolve_kubeconfig() {
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          if [ -r "$KUBECONFIG" ]; then
            log_info "Using KUBECONFIG=$KUBECONFIG"
            return 0
          else
            log_warn "KUBECONFIG=$KUBECONFIG exists but is not readable"
          fi
        fi
        if [ -f "$HOME/.kube/rke2.yaml" ] && [ -r "$HOME/.kube/rke2.yaml" ]; then
          KUBECONFIG="$HOME/.kube/rke2.yaml"; export KUBECONFIG
          log_info "Using user kubeconfig: $KUBECONFIG"; return 0
        fi
        if [ "''${CYBERSEC_K8S_TARGET:-}" = "k3d" ]; then
          KUBECONFIG="''${DEVENV_STATE:-.devenv/state}/kubeconfig"; export KUBECONFIG
          log_info "Using k3d kubeconfig: $KUBECONFIG"; return 0
        fi
        if [ -f "/etc/rancher/rke2/rke2.yaml" ]; then
          if [ -r "/etc/rancher/rke2/rke2.yaml" ]; then
            KUBECONFIG="/etc/rancher/rke2/rke2.yaml"; export KUBECONFIG
            log_info "Using RKE2 kubeconfig: $KUBECONFIG"; return 0
          else
            log_error "RKE2 kubeconfig not readable. Fix: sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$(id -u):\$(id -g) ~/.kube/rke2.yaml"
            return 1
          fi
        fi
        if [ -f "$HOME/.kube/config" ] && [ -r "$HOME/.kube/config" ]; then
          KUBECONFIG="$HOME/.kube/config"; export KUBECONFIG
          log_info "Using default kubeconfig: $KUBECONFIG"; return 0
        fi
        log_error "No kubeconfig found. Set KUBECONFIG."
        return 1
      }
      _resolve_kubeconfig || exit 1

      # --- Remove deployed Zarf packages (reverse order) ---
      log_info "Removing deployed Zarf packages..."

      if zarf package list 2>/dev/null | grep -q "cybersec-dask"; then
        log_info "Removing cybersec-dask package..."
        zarf package remove cybersec-dask --confirm 2>&1 | tail -5
        log_success "cybersec-dask package removed"
      else
        log_info "cybersec-dask package not deployed (skipping)"
      fi

      if zarf package list 2>/dev/null | grep -q "init"; then
        log_info "Removing zarf init package..."
        zarf package remove init --confirm 2>&1 | tail -5
        log_success "zarf init package removed"
      else
        log_info "zarf init not deployed (skipping)"
      fi

      # --- Clean up any remaining namespaces ---
      log_info "Cleaning up remaining namespaces..."
      for ns in panel-viz jupyterhub dask dask-operator zarf; do
        if kubectl get ns "$ns" &>/dev/null; then
          log_info "Deleting namespace: $ns"
          kubectl delete ns "$ns" --timeout=120s 2>/dev/null || \
            log_warn "Namespace $ns deletion timed out (may need manual cleanup)"
        fi
      done

      # --- Clean up Dask CRDs ---
      for crd in daskclusters.kubernetes.dask.org daskjobs.kubernetes.dask.org daskworkergroups.kubernetes.dask.org daskautoscalers.kubernetes.dask.org; do
        if kubectl get crd "$crd" &>/dev/null; then
          log_info "Deleting CRD: $crd"
          kubectl delete crd "$crd" 2>/dev/null || true
        fi
      done

      # --- Kill any lingering port-forwards ---
      pkill -f "kubectl port-forward.*panel-viz" 2>/dev/null || true
      pkill -f "kubectl port-forward.*dask" 2>/dev/null || true

      # --- Verify clean state ---
      echo ""
      log_info "Verifying clean state..."
      REMAINING=$(kubectl get ns --no-headers 2>/dev/null | grep -cE "dask|zarf|jupyter|panel" || true)
      if [ "$REMAINING" -eq 0 ]; then
        log_success "All Zarf-managed namespaces removed"
      else
        log_warn "Some namespaces still exist:"
        kubectl get ns --no-headers 2>/dev/null | grep -E "dask|zarf|jupyter|panel" | sed 's/^/  /'
      fi

      echo ""
      log_success "=== Reset Complete (clean slate) ==="
      echo ""
      echo "E2E deployment:"
      echo "  devenv tasks run zarf:local:preflight"
      echo "  devenv tasks run zarf:local:init"
      echo "  devenv tasks run zarf:local:deploy"
    '';

    "restart:clean".exec = ''
      source scripts/polaris_bootstrap_helper.sh

      # === Auto-bootstrap if needed ===
      FLINK_DIST="thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1"

      # Check if git submodules need initialization
      if [ ! -d "thirdparty/flink/.git" ] && [ ! -f "thirdparty/flink/pom.xml" ]; then
        log_info "=== First-time setup: Initializing git submodules ==="
        git submodule update --init --recursive
        log_success "Git submodules initialized"
      fi

      # Check if Flink needs to be built
      if [ ! -f "''${FLINK_DIST}/bin/flink" ]; then
        log_info "=== First-time setup: Building Flink from source ==="
        log_info "This takes 10-15 minutes on first run..."
        cd thirdparty/flink
        mvn clean install -DskipTests -Dfast -T 1C
        cd ../..
        if [ -f "''${FLINK_DIST}/bin/flink" ]; then
          log_success "Flink built successfully"
        else
          log_error "Flink build failed - check Maven output above"
          exit 1
        fi
      fi

      # Check if Polaris needs to be built
      POLARIS_HOME="thirdparty/polaris/polaris-bin-1.3.0-incubating"
      if [ ! -f "''${POLARIS_HOME}/server/quarkus-run.jar" ]; then
        log_info "=== First-time setup: Building Polaris from source ==="
        log_info "This takes 3-5 minutes on first run..."
        cd thirdparty/polaris
        ./gradlew :polaris-distribution:assemble -x test -x integrationTest

        # Extract the distribution
        DIST_TGZ="runtime/distribution/build/distributions/polaris-bin-1.3.0-incubating.tgz"
        if [ -f "$DIST_TGZ" ]; then
          tar -xzf "$DIST_TGZ" -C .
          log_success "Polaris distribution extracted"
        else
          log_error "Polaris build failed - distribution tarball not found"
          exit 1
        fi
        cd ../..

        # Create wrapper scripts
        if [ -f "scripts/setup_polaris_bin.sh" ]; then
          ./scripts/setup_polaris_bin.sh "$POLARIS_HOME"
        fi

        if [ -f "''${POLARIS_HOME}/server/quarkus-run.jar" ]; then
          log_success "Polaris built successfully"
        else
          log_error "Polaris build failed - server JAR not found"
          exit 1
        fi
      fi

      echo "Aggressively stopping all processes..."

      # Portable process killing function (works on both Linux and macOS)
      kill_by_pattern() {
        local pattern="''$1"
        # Use ps + grep for maximum portability (works on Linux and macOS)
        ps aux | grep -E "''$pattern" | grep -v grep | awk '{print ''$2}' | xargs kill -9 2>/dev/null || true
      }

      # Portable port killing function
      kill_by_port() {
        local port="''$1"
        if command -v lsof &> /dev/null; then
          lsof -ti:"''$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
        fi
      }

      # Kill process-compose and all related processes
      kill_by_pattern "process-compose"
      kill_by_pattern "iceberg-browser"
      kill_by_pattern "cloudtrail"
      kill_by_pattern "devenv-tasks"

      # Kill OTEL collector and Prometheus explicitly
      kill_by_pattern "otelcol"
      kill_by_pattern "prometheus"

      # Kill NiFi processes
      kill_by_pattern "org.apache.nifi"

      # Kill by port - ALL services:
      # 8181/8182: Polaris REST/Admin
      # 5438: PostgreSQL
      # 9010/9011: RustFS (local S3) API/Console
      # 8081: Flink
      # 5050: Iceberg Browser
      # 8450: NiFi
      # 4317/4318: OTEL gRPC/HTTP
      # 8888/8889: OTEL internal/Prometheus metrics
      # 9090: Prometheus
      # 9876: Cost Monitor
      # 8786/8787: Dask scheduler/dashboard (port-forward)
      # 10443: Kubernetes Dashboard (port-forward)
      # 6550: k3d API server (host port)
      # 8000: JupyterHub (port-forward)
      for port in 8181 8182 5438 9010 9011 8081 5050 8450 4317 4318 8888 8889 9090 9876 8786 8787 10443 6550 8000; do
        kill_by_port "$port"
      done

      # Kill remaining service processes using portable pattern matching
      kill_by_pattern "rustfs|minio|postgres|flink|taskmanager|jobmanager|quarkus|polaris|otelcol|nifi|cost.monitor"

      # Verify critical ports are released
      log_info "Verifying ports are released..."
      for port in 8181 8182 5438 8888 8889 9090; do
        wait_for_port_release $port 5 2 || log_warn "Port $port may still be in use"
      done

      # Clean up socket and temp files (portable path using TMPDIR)
      rm -f "$TMPDIR"/devenv-*/pc.sock 2>/dev/null || true
      rm -f /tmp/devenv-*/pc.sock 2>/dev/null || true
      rm -f /tmp/cloudtrail.log /tmp/cloudtrail.pid /tmp/polaris-init.log /tmp/polaris-catalog-init.log 2>/dev/null || true

      # macOS-specific: restart Podman machine and refresh connection
      if command -v podman >/dev/null 2>&1 && [ "$(uname -s)" = "Darwin" ]; then
        echo "Restarting Podman machine..."
        podman machine stop podman-machine-default >/dev/null 2>&1 || true
        podman machine start podman-machine-default >/dev/null 2>&1 || true
        if PODMAN_CONNS=$(podman system connection list --format json 2>/dev/null); then
          DEFAULT_CONN=$(echo "$PODMAN_CONNS" | jq -r 'map(select(.Default==true)) | .[0].Name // empty')
          if [ -z "$DEFAULT_CONN" ]; then
            FIRST_CONN=$(echo "$PODMAN_CONNS" | jq -r 'map(select(.ReadWrite==true)) | .[0].Name // empty')
            if [ -n "$FIRST_CONN" ]; then
              podman system connection default "$FIRST_CONN" >/dev/null 2>&1 || true
            fi
          fi
        fi
      fi

      # Clean up k3d cluster and kubeconfig artifacts
      if command -v k3d >/dev/null 2>&1; then
        k3d cluster delete cybersec >/dev/null 2>&1 || true
      fi
      rm -f "$DEVENV_STATE/kubeconfig" 2>/dev/null || true
      rm -f "$PWD/build/kubeconfig" 2>/dev/null || true
      rm -f "$PWD/build/kubeconfig-dashboard" 2>/dev/null || true

      # Clean up k3d Podman resources if available
      if command -v podman >/dev/null 2>&1; then
        PODMAN_CMD="podman"
        if [ -n "''${DOCKER_HOST:-}" ] && [ "''${DOCKER_HOST#unix://}" != "$DOCKER_HOST" ]; then
          SOCKET_PATH="''${DOCKER_HOST#unix://}"
          if [ -S "$SOCKET_PATH" ]; then
            PODMAN_CMD="podman --url unix://$SOCKET_PATH"
          fi
        fi
        $PODMAN_CMD rm -f k3d-cybersec-tools >/dev/null 2>&1 || true
        $PODMAN_CMD network rm k3d-cybersec >/dev/null 2>&1 || true
        $PODMAN_CMD volume rm k3d-cybersec-images >/dev/null 2>&1 || true
      fi

      echo "All processes killed and temp files cleaned"
      
      # Remove PostgreSQL data to trigger fresh initialization
      echo "Removing PostgreSQL data for fresh initialization..."
      if [ -d "$DEVENV_STATE/postgres" ]; then
        rm -rf "$DEVENV_STATE/postgres"
        echo "PostgreSQL data removed"
      else
        echo "No PostgreSQL data found to remove"
      fi
      
      echo "Note: Fresh PostgreSQL will initialize with polaris_schema"
      echo "Note: Polaris catalog will be created automatically on startup"
      sleep 2
      
      # Check if bootstrap is needed (missing connectors, etc.)
      echo "Checking bootstrap status..."
      ASSESS_OUTPUT=$(uv run python -c "
import asyncio
from cybersec.bootstrap.service import BootstrapService
async def check():
    svc = BootstrapService()
    result = await svc.assess()
    print('NEEDS_BOOTSTRAP=' + ('1' if result['needs_bootstrap'] else '0'))
    print('ICEBERG_CONNECTOR=' + ('1' if result.get('iceberg_connector_installed') else '0'))
    print('FLINK_INSTALLED=' + ('1' if result['flink_installed'] else '0'))
asyncio.run(check())
" 2>/dev/null || echo "NEEDS_BOOTSTRAP=1")

      if echo "$ASSESS_OUTPUT" | grep -q "NEEDS_BOOTSTRAP=1"; then
        log_warn "Bootstrap required - running bootstrap..."
        if echo "$ASSESS_OUTPUT" | grep -q "ICEBERG_CONNECTOR=0"; then
          log_info "Missing Iceberg connector - will download"
        fi
        # Run bootstrap (non-interactive - will use defaults/skip prompts)
        uv run python -c "
import asyncio
from cybersec.bootstrap.service import BootstrapService
async def run():
    svc = BootstrapService()
    async for event in svc.run(skip_flink=False, skip_nifi=False):
        if event.message:
            print(f'  {event.message}')
asyncio.run(run())
" 2>&1 | while read line; do echo "  $line"; done
        echo "✅ Bootstrap completed"
      else
        echo "✅ Bootstrap already complete"
      fi

      echo "🚀 Starting fresh stack with devenv up -d..."
      devenv up -d &

      # Wait for services and verify complete E2E pipeline
      sleep 10
      log_info "Waiting for services to start and verifying E2E pipeline..."
      if verify_e2e; then
        log_success "Clean restart and E2E verification completed successfully!"
        log_info "✓ All services running"
        log_info "✓ Polaris catalog initialized"
        log_info "✓ CloudTrail DataGen job running"
        log_info "✓ Events flowing to Iceberg Browser"
        exit 0
      else
        log_warn "E2E verification failed - check logs for details"
        log_warn "Continuing despite E2E verification failure"
        exit 0
      fi
    '';
  };


  # ============================================================================
  # OpenTelemetry Collector - Receives telemetry from all Gaius components
  # ============================================================================
  services.opentelemetry-collector = {
    enable = true;
    package = pkgs.opentelemetry-collector-contrib;  # Use contrib for prometheus exporter
    settings = {
      receivers = {
        otlp = {
          protocols = {
            grpc.endpoint = "0.0.0.0:4317";
            http.endpoint = "0.0.0.0:4318";
          };
        };
      };
      processors = {
        batch = {
          timeout = "5s";
          send_batch_size = 1000;
        };
      };
      exporters = {
        prometheus = {
          endpoint = "0.0.0.0:8889";
          namespace = "cybersec";
          resource_to_telemetry_conversion.enabled = true;
        };
        debug.verbosity = "basic";
        # Forward traces to NiFi ListenOTLP for flow visualization
        # NiFi receives OTel data on port 4319 via ListenOTLP processor
        otlphttp = {
          endpoint = "http://localhost:4319";
          tls.insecure = true;
        };
      };
      service = {
        pipelines = {
          traces = {
            receivers = ["otlp"];
            processors = ["batch"];
            exporters = ["debug" "otlphttp"];  # Forward to NiFi
          };
          metrics = {
            receivers = ["otlp"];
            processors = ["batch"];
            exporters = ["prometheus"];
          };
        };
      };
    };
  };

  # ============================================================================
  # Prometheus - Metrics storage and querying
  # ============================================================================
  services.prometheus = {
    enable = true;
    port = 9090;
    # Note: Prometheus binds to 0.0.0.0 by default when port is specified
    storage.retentionTime = "15d";
    scrapeConfigs = [
      {
        job_name = "otel-collector";
        scrape_interval = "1s";  # 1s scraping for real-time ObservePanel
        static_configs = [{
          targets = ["localhost:8889"];
        }];
      }
      {
        job_name = "cost-monitor";
        scrape_interval = "60s";  # 1-minute granularity for cost metrics
        static_configs = [{
          targets = ["localhost:9876"];
        }];
      }
    ];
  };
  # Process-compose managed services (Flink, Polaris, Dask, auxiliary tooling)
  processes = {
    podman-runtime = {
      exec = ''
        set -euo pipefail

        # Podman is only needed for k3d provisioning (not for RKE2)
        if [ "''${CYBERSEC_K8S_TARGET:-none}" != "k3d" ]; then
          echo "K8s target is ''${CYBERSEC_K8S_TARGET:-none}, not k3d - skipping podman"
          exit 0
        fi

        if ! command -v podman >/dev/null 2>&1; then
          echo "podman CLI not found in dev environment"
          exit 1
        fi

        echo "Ensuring Podman environment is available for k3d..."

        if podman machine list >/dev/null 2>&1; then
          MACHINE_JSON=$(podman machine list --format=json 2>/dev/null || echo "[]")
          MACHINE_NAME=$(echo "$MACHINE_JSON" | jq -r 'map(select(.Name != null)) | map(.Name)[0] // empty')
          if [ -n "$MACHINE_NAME" ]; then
            MACHINE_STATE=$(echo "$MACHINE_JSON" | jq -r --arg name "$MACHINE_NAME" '.[] | select(.Name==$name) | .State // ""')
            MACHINE_STATE=$(echo "$MACHINE_STATE" | tr '[:upper:]' '[:lower:]')
            if [ "$MACHINE_STATE" != "running" ]; then
              echo "Starting Podman machine '$MACHINE_NAME'..."
              podman machine start "$MACHINE_NAME" || true
            else
              echo "Podman machine '$MACHINE_NAME' already running"
            fi
          else
            echo "No Podman machines defined; assuming native runtime"
          fi
        else
          echo "Podman machine tooling unavailable (likely native Linux runtime)"
        fi

        while true; do
          sleep 300
        done
      '';
      process-compose = {
        disabled = true;  # Started via k8s:provision task
        availability = {
          restart = "always";
        };
      };
    };

    k3d-cluster = {
      exec = ''
        set -euo pipefail

        # k3d cluster provisioning - only for local k3d target (not RKE2)
        if [ "''${CYBERSEC_K8S_TARGET:-none}" != "k3d" ]; then
          echo "K8s target is ''${CYBERSEC_K8S_TARGET:-none}, not k3d - skipping k3d cluster provisioning"
          exit 0
        fi

        CLUSTER_NAME=''${K3D_CLUSTER_NAME:-cybersec}
        KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"

        export K3D_FIX_DNS=0

        if ! command -v k3d >/dev/null 2>&1; then
          echo "k3d CLI not found"
          exit 1
        fi

        if ! command -v kubectl >/dev/null 2>&1; then
          echo "kubectl CLI not found"
          exit 1
        fi

        if command -v podman >/dev/null 2>&1 && podman machine list >/dev/null 2>&1; then
          MACHINE_JSON=$(podman machine list --format=json 2>/dev/null || echo "[]")
          MACHINE_NAME=$(echo "$MACHINE_JSON" | jq -r 'map(select(.Name != null)) | map(.Name)[0] // empty')
          CONN_URI=""
          if CONN_JSON=$(podman system connection list --format json 2>/dev/null); then
            CONN_URI=$(echo "$CONN_JSON" | jq -r 'map(select(.Default==true)) | .[0].URI // empty')
            if [ -z "$CONN_URI" ] && [ -n "$MACHINE_NAME" ]; then
              CONN_URI=$(echo "$CONN_JSON" | jq -r --arg name "$MACHINE_NAME" 'map(select(.Name==$name)) | .[0].URI // empty')
            fi
          fi
          if [ -n "$CONN_URI" ]; then
            export DOCKER_HOST="$CONN_URI"
            export K3D_HIDE_WARNING_ROOTLESS=1
            echo "Using Podman connection $CONN_URI for k3d"
          elif [ -n "$MACHINE_NAME" ]; then
            SOCKET_PATH=$(podman machine inspect "$MACHINE_NAME" 2>/dev/null | jq -r '.[0].ConnectionInfo.PodmanSocket.Path // empty')
            if [ -n "$SOCKET_PATH" ] && [ -S "$SOCKET_PATH" ]; then
              export DOCKER_HOST="unix://$SOCKET_PATH"
              export K3D_HIDE_WARNING_ROOTLESS=1
              echo "Using Podman machine socket at $SOCKET_PATH for k3d"
            fi
          fi
        fi

        mkdir -p "$(dirname "$KUBECONFIG_PATH")"

        if command -v podman >/dev/null 2>&1 && [ -n "''${DOCKER_HOST:-}" ]; then
          PODMAN_CMD="podman"
          case "''${DOCKER_HOST}" in
            unix://*)
              SOCKET_PATH="''${DOCKER_HOST#unix://}"
              if [ -S "$SOCKET_PATH" ]; then
                PODMAN_CMD="podman --url unix://$SOCKET_PATH"
              fi
              ;;
            ssh://*|tcp://*)
              PODMAN_CMD="podman --url ''${DOCKER_HOST}"
              ;;
          esac
          NETWORK_NAME="k3d-$CLUSTER_NAME"
          IMAGE_VOLUME="k3d-$CLUSTER_NAME-images"
          TOOLS_NODE="k3d-$CLUSTER_NAME-tools"

            K3D_VERSION=""
            if K3D_VERSION_JSON=$(k3d version --output json 2>/dev/null); then
              case "$K3D_VERSION_JSON" in
                \{*\}|\[*\])
                  K3D_VERSION=$(echo "$K3D_VERSION_JSON" | jq -r '(.k3d.version? // .k3d? // empty)' | sed 's/^v//')
                  ;;
              esac
            fi
            if [ -z "$K3D_VERSION" ]; then
              K3D_VERSION=$(k3d version 2>/dev/null | awk '/k3d version/ {print $3}' | sed 's/^v//')
            fi
            TOOLS_IMAGE="''${K3D_IMAGE_TOOLS:-}"
            if [ -z "$TOOLS_IMAGE" ]; then
              if [ -n "''${K3D_HELPER_IMAGE_TAG:-}" ]; then
                TOOLS_IMAGE="ghcr.io/k3d-io/k3d-tools:''${K3D_HELPER_IMAGE_TAG}"
              elif [ -n "$K3D_VERSION" ]; then
                TOOLS_IMAGE="ghcr.io/k3d-io/k3d-tools:$K3D_VERSION"
              else
                TOOLS_IMAGE="ghcr.io/k3d-io/k3d-tools:latest"
              fi
            fi

          if ! $PODMAN_CMD network exists "$NETWORK_NAME" >/dev/null 2>&1; then
            $PODMAN_CMD network create "$NETWORK_NAME" >/dev/null
          fi

          if ! $PODMAN_CMD volume exists "$IMAGE_VOLUME" >/dev/null 2>&1; then
            $PODMAN_CMD volume create "$IMAGE_VOLUME" >/dev/null
          fi

          if $PODMAN_CMD container exists "$TOOLS_NODE" >/dev/null 2>&1; then
            EXISTING_LABEL=$($PODMAN_CMD inspect "$TOOLS_NODE" --format '{{index .Config.Labels "app"}}' 2>/dev/null || true)
            if [ "$EXISTING_LABEL" != "k3d" ]; then
              $PODMAN_CMD rm -f "$TOOLS_NODE" >/dev/null 2>&1 || true
            fi
          fi

          if ! $PODMAN_CMD container exists "$TOOLS_NODE" >/dev/null 2>&1; then
            $PODMAN_CMD run -d \
              --name "$TOOLS_NODE" \
              --label app=k3d \
              --network "$NETWORK_NAME" \
              -v "$IMAGE_VOLUME:/k3d/images" \
              "$TOOLS_IMAGE" noop >/dev/null
          fi
        fi

        CLUSTER_EXISTS=0
        if k3d cluster list | awk 'NR>1 {print $1}' | grep -qx "$CLUSTER_NAME"; then
          CLUSTER_EXISTS=1
        fi

        if [ "$CLUSTER_EXISTS" -eq 1 ]; then
          if k3d node list | awk 'NR>1 {print $1}' | grep -q "^k3d-$CLUSTER_NAME-serverlb$"; then
            echo "Existing cluster has serverlb; recreating to apply disabled load balancer..."
            k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
            CLUSTER_EXISTS=0
          fi
        fi

        if [ "$CLUSTER_EXISTS" -eq 1 ] && command -v lsof >/dev/null 2>&1; then
          if lsof -nP -iTCP:8786 -sTCP:LISTEN 2>/dev/null | grep -q "gvproxy" || \
             lsof -nP -iTCP:8787 -sTCP:LISTEN 2>/dev/null | grep -q "gvproxy"; then
            echo "Existing cluster is binding Dask ports via gvproxy; recreating to remove host port mappings..."
            k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
            CLUSTER_EXISTS=0
          fi
        fi

        if [ "$CLUSTER_EXISTS" -eq 0 ]; then
          echo "Creating k3d cluster '$CLUSTER_NAME'..."

          CONFIG_FILE=$(mktemp)
          cat > "$CONFIG_FILE" <<EOF
apiVersion: k3d.io/v1alpha5
kind: Simple
metadata:
  name: $CLUSTER_NAME
image: rancher/k3s:v1.31.6-k3s1
servers: 1
agents: 0
options:
  k3d:
    wait: true
    timeout: 300s
    disableLoadbalancer: true
  k3s:
    extraArgs:
      - arg: --disable=traefik
        nodeFilters:
          - server:*
      - arg: --disable=servicelb
        nodeFilters:
          - server:*
      - arg: --disable=traefik
        nodeFilters:
          - agent:*
      - arg: --disable=servicelb
        nodeFilters:
          - agent:*
EOF

          trap 'rm -f "$CONFIG_FILE"' EXIT
          k3d cluster create --api-port 0.0.0.0:6550 --config "$CONFIG_FILE"
          rm -f "$CONFIG_FILE"
          trap - EXIT
        else
          echo "k3d cluster '$CLUSTER_NAME' already exists; ensuring it is started..."
          k3d cluster start "$CLUSTER_NAME" || true
        fi

        TMP_KUBECONFIG="$KUBECONFIG_PATH.tmp"
        k3d kubeconfig get "$CLUSTER_NAME" > "$TMP_KUBECONFIG"
        mv "$TMP_KUBECONFIG" "$KUBECONFIG_PATH"
        chmod 600 "$KUBECONFIG_PATH"
        ln -sfn "$PWD/.devenv/state" "$PWD/kube-state"
        ln -sfn "$KUBECONFIG_PATH" "$PWD/kubeconfig"
        python -c "from pathlib import Path; path = Path(\"''${KUBECONFIG_PATH}\"); text = path.read_text(); text = text.replace('https://0.0.0.0:', 'https://127.0.0.1:'); text = text.replace('https://host.k3d.internal:', 'https://127.0.0.1:'); path.write_text(text)"
        export KUBECONFIG="$KUBECONFIG_PATH"

        echo "Waiting for Kubernetes API..."
        for _ in $(seq 1 60); do
          if kubectl get nodes >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done

        kubectl wait --for=condition=Ready node --all --timeout=300s

        echo "k3d cluster '$CLUSTER_NAME' is ready"

        while true; do
          if ! kubectl get nodes >/dev/null 2>&1; then
            echo "Lost connection to cluster; exiting for restart"
            exit 1
          fi
          sleep 30
        done
      '';
      process-compose = {
        disabled = true;  # Started via k8s:provision task
        depends_on = {
          podman-runtime = {
            condition = "process_started";
          };
        };
      };
    };

    dask-operator = {
      exec = ''
        set -euo pipefail

        # Dask operator runs on any K8s target (k3d or RKE2)
        # Use existing KUBECONFIG if set, otherwise fall back to k3d-generated config
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          KUBECONFIG_PATH="$KUBECONFIG"
        else
          KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "Waiting for kubeconfig at $KUBECONFIG_PATH..."
          for _ in $(seq 1 60); do
            if [ -f "$KUBECONFIG_PATH" ]; then
              break
            fi
            sleep 2
          done
        fi

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "kubeconfig not found at $KUBECONFIG_PATH"
          exit 1
        fi

        echo "Waiting for Kubernetes control plane before installing Dask operator..."
        for _ in $(seq 1 60); do
          if kubectl get namespace kube-system >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done

        helm repo add dask https://helm.dask.org >/dev/null 2>&1 || true
        helm repo update dask >/dev/null 2>&1 || true

        echo "Installing/Updating Dask Kubernetes operator via Helm..."
        helm upgrade --install dask-operator dask/dask-kubernetes-operator \
          --namespace dask-operator \
          --create-namespace \
          --wait \
          --timeout 5m

        kubectl wait --for=condition=Established crd/daskclusters.kubernetes.dask.org --timeout=120s
        kubectl wait --for=condition=Established crd/daskworkergroups.kubernetes.dask.org --timeout=120s

        echo "Dask operator installed"

        while true; do
          if ! kubectl get pods -n dask-operator >/dev/null 2>&1; then
            echo "Unable to query Dask operator pods; exiting for restart"
            exit 1
          fi
          sleep 30
        done
      '';
      process-compose = {
        disabled = true;  # Started via k8s:deploy-dask task
        depends_on = {
          k3d-cluster = {
            condition = "process_started";
          };
        };
      };
    };

    yunikorn = {
      exec = ''
        set -euo pipefail

        if [ "''${ENABLE_YUNIKORN:-false}" != "true" ]; then
          echo "YuniKorn disabled (set ENABLE_YUNIKORN=true to enable)"
          exit 0
        fi

        # Use existing KUBECONFIG if set, otherwise fall back to k3d-generated config
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          KUBECONFIG_PATH="$KUBECONFIG"
        else
          KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "Waiting for kubeconfig at $KUBECONFIG_PATH..."
          for _ in $(seq 1 60); do
            if [ -f "$KUBECONFIG_PATH" ]; then
              break
            fi
            sleep 2
          done
        fi

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "kubeconfig not found at $KUBECONFIG_PATH"
          exit 1
        fi

        echo "Waiting for Kubernetes control plane before installing YuniKorn..."
        for _ in $(seq 1 60); do
          if kubectl get namespace kube-system >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done

        helm repo add yunikorn https://apache.github.io/yunikorn-release >/dev/null 2>&1 || true
        helm repo update yunikorn >/dev/null 2>&1 || true

        echo "Installing/Updating YuniKorn scheduler via Helm..."
        helm upgrade --install yunikorn yunikorn/yunikorn \
          --namespace yunikorn \
          --create-namespace \
          --version 1.8.0 \
          --wait \
          --timeout 5m

        kubectl wait --for=condition=Available deployment/yunikorn-scheduler -n yunikorn --timeout=120s

        echo "YuniKorn scheduler installed"

        while true; do
          if ! kubectl get pods -n yunikorn >/dev/null 2>&1; then
            echo "Unable to query YuniKorn pods; exiting for restart"
            exit 1
          fi
          sleep 30
        done
      '';
      process-compose = {
        disabled = true;  # Optional, enabled via ENABLE_YUNIKORN
        depends_on = {
          dask-operator = {
            condition = "process_started";
          };
        };
      };
    };

    dask-cluster = {
      exec = ''
        set -euo pipefail

        # Dask cluster runs on any K8s target (k3d or RKE2)
        if [ "''${ENABLE_YUNIKORN:-false}" = "true" ]; then
          MANIFEST="$PWD/infra/dask/dask-cluster-yunikorn.yaml"
        else
          MANIFEST="$PWD/infra/dask/dask-cluster.yaml"
        fi
        if [ ! -f "$MANIFEST" ]; then
          echo "Dask manifest not found at $MANIFEST"
          exit 1
        fi

        # Use existing KUBECONFIG if set, otherwise fall back to k3d-generated config
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          KUBECONFIG_PATH="$KUBECONFIG"
        else
          KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        echo "Waiting for Dask operator CRDs..."
        for _ in $(seq 1 60); do
          if kubectl get crd daskclusters.kubernetes.dask.org >/dev/null 2>&1 && \
             kubectl get crd daskworkergroups.kubernetes.dask.org >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done

        kubectl apply -f "$MANIFEST"

        echo "Waiting for Dask cluster to reach Running phase..."
        STATUS=""
        for _ in $(seq 1 120); do
          STATUS=$(kubectl get daskcluster cybersec-dask -n dask -o jsonpath='{.status.phase}' 2>/dev/null || true)
          if [ "$STATUS" = "Running" ]; then
            break
          fi
          sleep 5
        done

        if [ "$STATUS" != "Running" ]; then
          echo "Dask cluster did not reach Running status (last status: ''${STATUS:-unknown})"
          kubectl get daskclusters -n dask || true
          exit 1
        fi

        echo "Dask cluster is running"
        kubectl get svc -n dask cybersec-dask-scheduler || true

        while true; do
          CURRENT=$(kubectl get daskcluster cybersec-dask -n dask -o jsonpath='{.status.phase}' 2>/dev/null || true)
          if [ "$CURRENT" != "Running" ]; then
            echo "Dask cluster status is '$CURRENT'; exiting for restart"
            exit 1
          fi
          sleep 30
        done
      '';
      process-compose = {
        disabled = true;  # Started via k8s:deploy-dask task
        depends_on = {
          dask-operator = {
            condition = "process_started";
          };
        };
      };
    };

    k8s-dashboard = {
      exec = ''
        set -euo pipefail

        # Port-forward only — deployment handled by Zarf package:
        #   devenv tasks run zarf:local:deploy-dashboard

        # Try kubeconfig locations in priority order
        KUBECONFIG_PATH=""
        for candidate in \
          "$(grep '^KUBECONFIG=' "$PWD/.env" 2>/dev/null | cut -d= -f2-)" \
          "$HOME/.kube/rke2.yaml" \
          "''${KUBECONFIG:-}" \
          "$PWD/.devenv/state/kubeconfig"; do
          if [ -n "$candidate" ] && [ -f "$candidate" ]; then
            KUBECONFIG_PATH="$candidate"
            break
          fi
        done

        if [ -z "$KUBECONFIG_PATH" ]; then
          echo "No kubeconfig found — skipping dashboard port-forward."
          sleep infinity
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        # Wait for dashboard namespace to exist (deployed by Zarf)
        echo "Waiting for kubernetes-dashboard namespace..."
        for _ in $(seq 1 60); do
          if kubectl get ns kubernetes-dashboard >/dev/null 2>&1; then
            break
          fi
          sleep 5
        done

        if ! kubectl get ns kubernetes-dashboard >/dev/null 2>&1; then
          echo "kubernetes-dashboard not deployed."
          echo "Deploy with: devenv tasks run zarf:local:deploy-dashboard"
          sleep infinity
        fi

        # Wait for dashboard pod to be ready
        kubectl -n kubernetes-dashboard rollout status deploy/kubernetes-dashboard --timeout=120s || true

        echo "Starting Kubernetes Dashboard port-forward on https://localhost:10443 ..."
        echo "  Generate login token:  kubectl -n kubernetes-dashboard create token admin-user"
        echo "  Or use Settings page:  http://localhost:5050/settings"
        kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard 10443:443 --address 0.0.0.0,::1
      '';
      process-compose = {
        # Starts automatically; sleeps if dashboard not deployed
        availability = {
          restart = "on_failure";
          max_restarts = 3;
        };
      };
    };

    jupyterhub = {
      exec = ''
        set -euo pipefail

        # JupyterHub runs on any K8s target (k3d or RKE2)
        # Use existing KUBECONFIG if set, otherwise fall back to k3d-generated config
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          KUBECONFIG_PATH="$KUBECONFIG"
        else
          KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "Waiting for kubeconfig at $KUBECONFIG_PATH..."
          for _ in $(seq 1 60); do
            if [ -f "$KUBECONFIG_PATH" ]; then
              break
            fi
            sleep 2
          done
        fi

        if [ ! -f "$KUBECONFIG_PATH" ]; then
          echo "kubeconfig not found at $KUBECONFIG_PATH"
          exit 1
        fi

        echo "Waiting for Kubernetes API..."
        for _ in $(seq 1 60); do
          if kubectl get namespace kube-system >/dev/null 2>&1; then
            break
          fi
          sleep 2
        done

        echo "Installing/Updating JupyterHub via Helm..."
        helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/ >/dev/null 2>&1 || true
        helm repo update jupyterhub >/dev/null 2>&1 || true
        NOTEBOOK_PATH="$PWD/build/Dask_Kub_Viz_Sample_Problem.ipynb"
        EXTRA_ARGS=()
        if [ -f "$NOTEBOOK_PATH" ]; then
          EXTRA_ARGS+=(--set-file "singleuser.extraFiles.dask_notebook.stringData=$NOTEBOOK_PATH")
          EXTRA_ARGS+=(--set "singleuser.extraFiles.dask_notebook.mountPath=/home/jovyan/Dask_Kub_Viz_Sample_Problem.ipynb")
          EXTRA_ARGS+=(--set "singleuser.extraFiles.dask_notebook.mode=420")
        fi
        EXTRA_ARGS+=(--set-json "singleuser.lifecycleHooks.postStart.exec.command=[\"/bin/sh\",\"-c\",\"pip install --quiet holoviews datashader bokeh dask[distributed] pyarrow\"]")
        helm upgrade --install jupyterhub jupyterhub/jupyterhub \
          --version 2.0.0 \
          --namespace jupyterhub \
          --create-namespace \
          --set-json singleuser.cpu.guarantee=0.1 \
          --set-json singleuser.cpu.limit=0.5 \
          --set-string singleuser.memory.guarantee=256M \
          --set-string singleuser.memory.limit=512M \
          --set singleuser.networkPolicy.enabled=false \
          "''${EXTRA_ARGS[@]}"

        kubectl -n jupyterhub rollout status deploy/hub --timeout=300s || true
        kubectl -n jupyterhub rollout status deploy/proxy --timeout=300s || true

        echo "Starting JupyterHub on http://localhost:8000 ..."
        kubectl -n jupyterhub port-forward svc/proxy-public 8000:80 --address 127.0.0.1,::1
      '';
      process-compose = {
        disabled = true;  # Started via k8s:deploy-jupyter task
        depends_on = {
          k3d-cluster = {
            condition = "process_started";
          };
        };
      };
    };

    dask-ui = {
      exec = ''
        set -euo pipefail

        # Dask UI runs on any K8s target (k3d or RKE2)
        # Use existing KUBECONFIG if set, otherwise fall back to k3d-generated config
        if [ -n "''${KUBECONFIG:-}" ] && [ -f "$KUBECONFIG" ]; then
          KUBECONFIG_PATH="$KUBECONFIG"
        else
          KUBECONFIG_PATH="$PWD/.devenv/state/kubeconfig"
        fi
        export KUBECONFIG="$KUBECONFIG_PATH"

        echo "Waiting for Dask scheduler pod..."
        for _ in $(seq 1 120); do
          SCHEDULER_POD=$(kubectl -n dask get pods -l dask.org/component=scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
          if [ -n "$SCHEDULER_POD" ]; then
            READY=$(kubectl -n dask get pod "$SCHEDULER_POD" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
            if [ "$READY" = "true" ]; then
              break
            fi
          fi
          sleep 2
        done

        if [ -z "''${SCHEDULER_POD:-}" ]; then
          echo "Dask scheduler pod not found"
          exit 1
        fi

        echo "Starting Dask UI on http://localhost:8787 ..."
        kubectl -n dask port-forward pod/$SCHEDULER_POD 8787:8787 --address 127.0.0.1,::1
      '';
      process-compose = {
        disabled = true;  # Started via k8s:forward task
        depends_on = {
          dask-cluster = {
            condition = "process_started";
          };
        };
      };
    };

    # Bootstrap check - runs on startup to verify environment
    bootstrap-check = {
      exec = ''
        echo "======================================================"
        echo "  Cyberphy Bootstrap Check"
        echo "======================================================"

        # Shared lab helpers (RustFS, kubeconfig, package finder)
        if [ -f "$PWD/scripts/lab_env.sh" ]; then
          # shellcheck source=/dev/null
          source "$PWD/scripts/lab_env.sh"
          ensure_local_s3_env
        fi

        # --- Plane A: devenv lab (RustFS + Polaris cyberphy) ---
        echo ""
        echo "  Lab services:"
        S3_PORT="''${LOCAL_S3_PORT:-9010}"
        if command -v rustfs_up >/dev/null 2>&1 && rustfs_up "http://127.0.0.1:''${S3_PORT}"; then
          echo "  RustFS:     OK (:''${S3_PORT}, bucket=''${S3_BUCKET:-cyberphy})"
        elif curl -sf --max-time 2 "http://127.0.0.1:''${S3_PORT}/health" >/dev/null 2>&1; then
          echo "  RustFS:     OK (:''${S3_PORT})"
        else
          echo "  RustFS:     DOWN (:''${S3_PORT}) — start with: devenv up"
        fi

        CATALOG="''${POLARIS_CATALOG_NAME:-cyberphy}"
        if curl -sf --max-time 2 http://127.0.0.1:8182/q/health/ready >/dev/null 2>&1; then
          # Best-effort catalog name hint from env / warehouse
          echo "  Polaris:    OK (catalog=''${CATALOG}, warehouse=''${POLARIS_WAREHOUSE:-s3://cyberphy/iceberg/warehouse})"
        else
          echo "  Polaris:    DOWN — polaris-init runs after postgres/rustfs"
        fi

        # --- Plane B: K8s / Zarf readiness ---
        echo ""
        echo "  K8s / Zarf:"
        if command -v resolve_kubeconfig >/dev/null 2>&1 && resolve_kubeconfig 2>/dev/null; then
          TGT=$(detect_k8s_target 2>/dev/null || echo unknown)
          echo "  KUBECONFIG: $KUBECONFIG (target=$TGT)"
          if kubectl --kubeconfig="$KUBECONFIG" get ns zarf >/dev/null 2>&1; then
            echo "  Zarf:       initialized (ns=zarf)"
          else
            echo "  Zarf:       not initialized — devenv tasks run zarf:local:init"
          fi
        else
          echo "  KUBECONFIG: (none usable)"
          if systemctl is-active --quiet rke2-server 2>/dev/null; then
            echo "  RKE2:       active but kubeconfig unreadable"
            echo "              sudo cp /etc/rancher/rke2/rke2.yaml ~/.kube/rke2.yaml && sudo chown \$USER ~/.kube/rke2.yaml"
          else
            echo "  RKE2:       inactive (optional for pure lab)"
          fi
        fi
        if command -v find_zarf_package >/dev/null 2>&1 && pkg=$(find_zarf_package 2>/dev/null); then
          echo "  Package:    $pkg"
        else
          echo "  Package:    (none — build/cyberphy-release-mirror/*/ or zarf/)"
        fi

        # Quick assessment using the bootstrap CLI
        if command -v python &> /dev/null; then
          python -c "
import sys
sys.path.insert(0, '.')
try:
    import asyncio
    from cybersec.bootstrap import BootstrapService
    service = BootstrapService()
    result = asyncio.run(service.assess())

    print()
    if result.get('config_exists'):
        print('  Config:     OK (.cybersec/config.toml exists)')
    else:
        print('  Config:     Missing (.cybersec/config.toml)')

    flink = result.get('flink_installed', False)
    flink_home = result.get('flink_home')
    if flink and flink_home:
        print(f'  Flink:      OK ({flink_home})')
    else:
        print('  Flink:      Not configured')

    tools = result.get('tools', {})
    missing = [t for t, found in tools.items() if not found]
    if missing:
        print(f'  Tools:      Missing: {missing}')
    else:
        print('  Tools:      OK (all required tools found)')

    print()
    if result.get('ready'):
        print('  Status: Environment is READY')
    elif result.get('needs_bootstrap'):
        print('  Status: Bootstrap REQUIRED')
        print()
        print('  Next steps:')
        print('    1. Open http://localhost:5050/settings in your browser')
        print('    2. Or run: cyberphy bootstrap run')
        print('    3. Or run: uv run python -m cybersec.cli.main bootstrap run')
        print('    4. Lab status: devenv tasks run lab:status')
    print()
except ImportError as e:
    print(f'  Bootstrap module not installed: {e}')
    print('  Run: uv pip install -e .')
    print()
except Exception as e:
    print(f'  Error during assessment: {e}')
    print()
"
        else
          echo "  Python not found - skipping bootstrap assessment"
        fi

        echo "======================================================"
        # One-shot process - exits after check
        exit 0
      '';
      process-compose = {
        availability = {
          restart = "no";
        };
      };
    };

    # Build Flink and install connectors if needed (one-shot process)
    flink-bootstrap = {
      exec = ''
        FLINK_VERSION="1.20.1"
        FLINK_DIST="$PWD/thirdparty/flink/flink-dist/target/flink-''${FLINK_VERSION}-bin/flink-''${FLINK_VERSION}"

        # Check if Flink is already built
        if [ -x "$FLINK_DIST/bin/flink" ]; then
          echo "Flink $FLINK_VERSION already built"
        else
          echo "Building Flink $FLINK_VERSION from source (this takes 10-15 minutes)..."

          # Ensure submodules are initialized
          if [ ! -f "thirdparty/flink/pom.xml" ]; then
            echo "Initializing git submodules..."
            git submodule update --init --recursive
          fi

          cd thirdparty/flink
          mvn clean install -DskipTests -Dfast -T 1C
          cd ../..

          if [ -x "$FLINK_DIST/bin/flink" ]; then
            echo "Flink built successfully"
          else
            echo "Flink build failed"
            exit 1
          fi
        fi

        # Install Iceberg connectors if needed
        ICEBERG_JAR=$(ls "$FLINK_DIST/lib/iceberg-flink-runtime-1.20-"*.jar 2>/dev/null | head -1)
        AWS_BUNDLE_JAR=$(ls "$FLINK_DIST/lib/iceberg-aws-bundle-"*.jar 2>/dev/null | head -1)
        if [ -z "$ICEBERG_JAR" ] || [ -z "$AWS_BUNDLE_JAR" ]; then
          echo "🔧 Installing Iceberg connectors..."

          # Ensure Iceberg submodule is initialized
          if [ ! -f "thirdparty/iceberg/gradlew" ]; then
            echo "  Initializing Iceberg submodule..."
            git submodule update --init thirdparty/iceberg
          fi

          # Build and install Iceberg JARs
          if [ -f "thirdparty/iceberg/gradlew" ]; then
            echo "  Building Iceberg Flink runtime and AWS bundle..."
            cd thirdparty/iceberg
            ./gradlew -PflinkVersions=1.20 \
              :iceberg-flink:iceberg-flink-runtime-1.20:shadowJar \
              :iceberg-aws-bundle:shadowJar \
              -x test -x integrationTest -x generateGitProperties \
              --no-daemon 2>&1 | grep -E "(BUILD|Task|WARN|ERROR)" || true

            # Copy Flink runtime JAR
            for jar in flink/v1.20/flink-runtime/build/libs/iceberg-flink-runtime-1.20-*.jar; do
              if [ -f "$jar" ] && [[ "$jar" != *"-sources.jar" ]] && [[ "$jar" != *"-javadoc.jar" ]]; then
                cp "$jar" "$FLINK_DIST/lib/"
                echo "Installed: $(basename $jar)"
                break
              fi
            done

            # Copy AWS bundle JAR
            for jar in aws-bundle/build/libs/iceberg-aws-bundle-*.jar; do
              if [ -f "$jar" ] && [[ "$jar" != *"-sources.jar" ]] && [[ "$jar" != *"-javadoc.jar" ]]; then
                cp "$jar" "$FLINK_DIST/lib/"
                echo "Installed: $(basename $jar)"
                break
              fi
            done

            cd ../..
          else
            echo "Iceberg submodule not available - run: git submodule update --init thirdparty/iceberg"
          fi

          # Verify installation
          ICEBERG_JAR=$(ls "$FLINK_DIST/lib/iceberg-flink-runtime-1.20-"*.jar 2>/dev/null | head -1)
          AWS_BUNDLE_JAR=$(ls "$FLINK_DIST/lib/iceberg-aws-bundle-"*.jar 2>/dev/null | head -1)
          if [ -z "$ICEBERG_JAR" ] || [ -z "$AWS_BUNDLE_JAR" ]; then
            echo "Iceberg JAR installation failed"
            echo "Missing: iceberg-flink-runtime and/or iceberg-aws-bundle"
            echo "Run: /health fix --apply"
            exit 1
          fi
        else
          echo "Iceberg connectors already installed"
        fi

        # Install S3 filesystem plugin (must be in plugins/, NOT lib/, to avoid delegation token conflict)
        S3_PLUGIN_DIR="$FLINK_DIST/plugins/s3-fs-hadoop"
        if [ ! -f "$S3_PLUGIN_DIR/flink-s3-fs-hadoop-1.20.1.jar" ]; then
          if [ -f "$FLINK_DIST/opt/flink-s3-fs-hadoop-1.20.1.jar" ]; then
            mkdir -p "$S3_PLUGIN_DIR"
            cp "$FLINK_DIST/opt/flink-s3-fs-hadoop-1.20.1.jar" "$S3_PLUGIN_DIR/"
            echo "Installed flink-s3-fs-hadoop as plugin (S3 filesystem support)"
          fi
        fi
        # Remove from lib/ if previously installed there (causes delegation token conflict)
        rm -f "$FLINK_DIST/lib/flink-s3-fs-hadoop-1.20.1.jar" 2>/dev/null || true

        if [ ! -f "$FLINK_DIST/lib/flink-python-1.20.1.jar" ]; then
          if [ -f "$FLINK_DIST/opt/flink-python-1.20.1.jar" ]; then
            cp "$FLINK_DIST/opt/flink-python-1.20.1.jar" "$FLINK_DIST/lib/"
            echo "Copied flink-python to lib (PyFlink support)"
          fi
        fi

        # Install Hadoop client JARs (needed by Iceberg FlinkCatalogFactory)
        # Uses hadoop-client-api (unshaded public API) + hadoop-client-runtime (shaded transitive deps)
        # instead of bare hadoop-common + individual transitive deps (which was incomplete).
        # Resolved via thirdparty/hadoop-client/build.gradle using the Iceberg gradlew.
        HADOOP_API_JAR=$(ls "$FLINK_DIST/lib/hadoop-client-api-"*.jar 2>/dev/null | head -1)
        HADOOP_RUNTIME_JAR=$(ls "$FLINK_DIST/lib/hadoop-client-runtime-"*.jar 2>/dev/null | head -1)
        if [ -z "$HADOOP_API_JAR" ] || [ -z "$HADOOP_RUNTIME_JAR" ]; then
          # Clean up old bare Hadoop JARs (from previous cherry-picking approach)
          for old_jar in hadoop-common-*.jar hadoop-auth-*.jar \
                         hadoop-hdfs-client-*.jar hadoop-shaded-guava-*.jar \
                         woodstox-core-*.jar stax2-api-*.jar; do
            rm -f "$FLINK_DIST/lib/$old_jar" 2>/dev/null
          done

          echo "Resolving Hadoop client JARs..."
          thirdparty/iceberg/gradlew -p thirdparty/hadoop-client \
            copyJars -PoutputDir="$FLINK_DIST/lib" \
            --no-daemon 2>&1 | grep -v "^$" || true
        else
          echo "Hadoop client JARs already installed"
        fi

        # Only remove AWS SDK bundle if it conflicts with iceberg-aws-bundle
        # NOTE: With flink-s3-fs-hadoop in plugins/, Hadoop JARs no longer conflict
        for jar in "$FLINK_DIST/lib/"aws-java-sdk-bundle-*.jar; do
          if [ -f "$jar" ]; then
            rm -f "$jar"
            echo "Removed conflicting JAR: $(basename $jar)"
          fi
        done

        # Seed a relocatable conf overlay. Host-specific keys (python.executable)
        # belong here, never in thirdparty/flink/.../target/.../conf/.
        FLINK_CONF_DIR="''${FLINK_CONF_DIR:-$DEVENV_STATE/flink/conf}"
        mkdir -p "$FLINK_CONF_DIR"
        if [ ! -f "$FLINK_CONF_DIR/config.yaml" ] && [ -f "$FLINK_DIST/conf/config.yaml" ]; then
          cp -a "$FLINK_DIST/conf/." "$FLINK_CONF_DIR/"
          echo "Seeded FLINK_CONF_DIR=$FLINK_CONF_DIR from dist (portable overlay)"
        fi

        echo "Flink bootstrap complete"
        exit 0
      '';
      process-compose = {
        availability = {
          restart = "no";
        };
      };
    };

    # Build flink-cyber Java datagen JAR if not already built (one-shot process)
    java-datagen-bootstrap = {
      exec = ''
        JAR="$PWD/flink-cyber/flink-common/target/flink-common-2.4.0.jar"
        if [ -f "$JAR" ]; then
          echo "Java datagen JAR exists: $JAR"
          exit 0
        fi
        echo "Building flink-cyber Java datagen..."
        cd flink-cyber && mvn clean install -DskipTests -pl flink-common -am
        if [ -f "$JAR" ]; then
          echo "Java datagen JAR built successfully"
          exit 0
        else
          echo "Failed to build Java datagen JAR"
          exit 1
        fi
      '';
      process-compose = {
        availability = {
          restart = "no";
        };
        depends_on = {
          flink-bootstrap = {
            condition = "process_completed_successfully";
          };
        };
      };
    };

    flink-jobmanager = {
      exec = ''
        # Use custom-built Apache Flink 1.20.1 (for Iceberg compatibility)
        export FLINK_HOME="''${FLINK_HOME:-$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1}"
        export FLINK_STATE_DIR="''${FLINK_STATE_DIR:-$DEVENV_STATE/flink}"
        export FLINK_CONF_DIR="''${FLINK_CONF_DIR:-$DEVENV_STATE/flink/conf}"
        export HADOOP_CONF_DIR="''${HADOOP_CONF_DIR:-$FLINK_CONF_DIR}"
        mkdir -p "$FLINK_STATE_DIR"/{logs,checkpoints,savepoints} "$FLINK_CONF_DIR"

        # Verify Flink exists
        if [ ! -x "$FLINK_HOME/bin/jobmanager.sh" ]; then
          echo "Flink not found at $FLINK_HOME"
          echo "Run: devenv tasks run restart:clean"
          exit 1
        fi

        # Add comprehensive Java module opens for checkpoint serialization
        export FLINK_ENV_JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED"

        # Run JobManager in foreground mode
        # classloader.parent-first-patterns: Fix Dropwizard metrics classloader conflict with Iceberg
        exec "$FLINK_HOME/bin/jobmanager.sh" start-foreground \
          -D jobmanager.rpc.address=localhost \
          -D rest.bind-address=0.0.0.0 \
          -D rest.port=8081 \
          -D jobmanager.memory.process.size=1024m \
          -D state.checkpoints.dir=file://$FLINK_STATE_DIR/checkpoints \
          -D state.savepoints.dir=file://$FLINK_STATE_DIR/savepoints \
          -D 'classloader.parent-first-patterns.additional=com.codahale.metrics;org.apache.flink.dropwizard'
      '';
      process-compose = {
        depends_on = {
          flink-bootstrap = {
            condition = "process_completed_successfully";
          };
        };
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 8081;
            path = "/overview";
          };
          initial_delay_seconds = 5;
          period_seconds = 2;
          failure_threshold = 30;
        };
      };
    };

    flink-taskmanager = {
      exec = ''
        # Use custom-built Apache Flink 1.20.1 (for Iceberg compatibility)
        export FLINK_HOME="''${FLINK_HOME:-$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1}"
        export FLINK_STATE_DIR="''${FLINK_STATE_DIR:-$DEVENV_STATE/flink}"
        export FLINK_CONF_DIR="''${FLINK_CONF_DIR:-$DEVENV_STATE/flink/conf}"
        mkdir -p "$FLINK_STATE_DIR"/{logs,tmp} "$FLINK_CONF_DIR"

        # Configure S3A for MinIO
        export HADOOP_CONF_DIR="''${HADOOP_CONF_DIR:-$FLINK_CONF_DIR}"
        
        # Add comprehensive Java module opens for checkpoint serialization
        export FLINK_ENV_JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED"
        
        # Run TaskManager in foreground mode
        # classloader.parent-first-patterns: Fix Dropwizard metrics classloader conflict with Iceberg
        exec "$FLINK_HOME/bin/taskmanager.sh" start-foreground \
          -D jobmanager.rpc.address=localhost \
          -D taskmanager.numberOfTaskSlots=4 \
          -D taskmanager.memory.process.size=8192m \
          -D taskmanager.memory.task.heap.size=4096m \
          -D taskmanager.memory.managed.fraction=0.1 \
          -D taskmanager.memory.jvm-overhead.fraction=0.1 \
          -D taskmanager.memory.network.fraction=0.1 \
          -D taskmanager.memory.task.off-heap.size=512m \
          -D taskmanager.tmp.dirs=$FLINK_STATE_DIR/tmp \
          -D 'classloader.parent-first-patterns.additional=com.codahale.metrics;org.apache.flink.dropwizard'
      '';
      process-compose = {
        depends_on = {
          flink-jobmanager = {
            condition = "process_healthy";
          };
        };
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 8081;
            path = "/taskmanagers";
          };
          initial_delay_seconds = 10;
          period_seconds = 3;
          failure_threshold = 20;
        };
      };
    };

    iceberg-browser = {
      exec = ''
        # Wait for required services
        echo "Starting Iceberg Browser..."
        echo "Web UI will be available at http://localhost:5050"
        
        # Run the Flask application
        exec python iceberg_browser.py
      '';
      process-compose = {
        depends_on = {
          polaris = {
            condition = "process_healthy";
          };
        };
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 5050;
            path = "/";
          };
          initial_delay_seconds = 3;
          period_seconds = 2;
          failure_threshold = 15;
        };
      };
    };

    # ============================================================================
    # Cost Monitor - AWS Cost Observability
    # ============================================================================
    # Polls AWS Cost Explorer and resource inventory, exposes Prometheus metrics.
    # Metrics endpoint: http://localhost:9876/metrics
    cost-monitor = {
      exec = ''
        echo "Starting AWS cost monitor..."
        echo "Metrics endpoint: http://localhost:9876/metrics"
        echo "Poll interval: 300 seconds (5 minutes)"

        # Use real AWS credentials (not MinIO)
        # AWS_PROFILE reads from ~/.aws/credentials
        export AWS_PROFILE="''${AWS_PROFILE:-default}"
        export AWS_REGION="''${AWS_REGION:-us-east-1}"

        # Run the cost monitor (Flask + Prometheus metrics)
        exec uv run python -m cybersec.cost.monitor
      '';
      process-compose = {
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 9876;
            path = "/health";
          };
          initial_delay_seconds = 10;
          period_seconds = 30;
          failure_threshold = 3;
        };
        availability = {
          restart = "on_failure";
          max_restarts = 5;
        };
      };
    };

    cloudtrail-datagen = {
      exec = ''
        echo "Starting CloudTrail DataGen job..."

        # Set Flink paths (repo-relative; FLINK_HOME may already be set by devenv)
        export FLINK_HOME="''${FLINK_HOME:-$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1}"
        export FLINK_STATE_DIR="''${FLINK_STATE_DIR:-$DEVENV_STATE/flink}"
        FLINK_BIN="$FLINK_HOME/bin/flink"
        mkdir -p "$FLINK_STATE_DIR/checkpoints"

        # Use uv venv Python which has PyFlink installed
        if [ -f "$PWD/.devenv/state/venv/bin/python3" ]; then
          PYCLIENT="$PWD/.devenv/state/venv/bin/python3"
        else
          PYCLIENT="python3"
        fi

        # Add comprehensive Java module opens for checkpoint serialization
        export FLINK_ENV_JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED"

        # Function to check if job is already running
        check_job_running() {
          curl -s http://localhost:8081/jobs/overview 2>/dev/null | \
            jq -e '.jobs[] | select(.name == "insert-into_cybersec.default.cloudtrail_events" and (.state == "RUNNING" or .state == "RESTARTING" or .state == "CREATED" or .state == "INITIALIZING"))' > /dev/null
        }

        # Check if CloudTrail DataGen is already running
        if check_job_running; then
          echo "CloudTrail DataGen job is already running. Monitoring..."
          # Keep process alive and monitor job status
          while true; do
            if ! check_job_running; then
              echo "Job stopped. Resubmitting..."
              "$FLINK_BIN" run -pyclientexec "$PYCLIENT" -py flink_jobs/cloudtrail_datagen.py
            fi
            sleep 30
          done
        else
          echo "Submitting CloudTrail DataGen job to Flink cluster using flink run..."
          "$FLINK_BIN" run -pyclientexec "$PYCLIENT" -py flink_jobs/cloudtrail_datagen.py

          # Monitor the job and keep process alive
          while true; do
            if ! check_job_running; then
              echo "Job stopped. Resubmitting..."
              "$FLINK_BIN" run -pyclientexec "$PYCLIENT" -py flink_jobs/cloudtrail_datagen.py
            fi
            sleep 30
          done
        fi
      '';
      process-compose = {
        disabled = false;  # Python datagen (10 rows/sec) - runs alongside Java datagen
        availability = {
          restart = "on_failure";
          max_restarts = 3;
        };
        depends_on = {
          flink-taskmanager = {
            condition = "process_healthy";
          };
          polaris-init = {
            condition = "process_completed_successfully";
          };
        };
      };
    };

    # Java CloudTrail DataGen - Pure Java pipeline for benchmarking
    # Generates synthetic CloudTrail events and writes directly to Iceberg
    java-cloudtrail-datagen = {
      exec = ''
        echo "Starting Java CloudTrail DataGen job..."

        # Set Flink paths
        export FLINK_HOME="''${FLINK_HOME:-$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1}"
        export FLINK_CONF_DIR="''${FLINK_CONF_DIR:-$DEVENV_STATE/flink/conf}"
        FLINK_BIN="$FLINK_HOME/bin/flink"
        JAR="''${FLINK_COMMON_JAR:-$PWD/flink-cyber/flink-common/target/flink-common-2.4.0.jar}"

        # Configurable rows per second (default 100 for benchmarking)
        RPS="''${JAVA_DATAGEN_RPS:-100}"

        # Add comprehensive Java module opens for checkpoint serialization
        export FLINK_ENV_JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED"

        # Function to check if job is already running
        check_job_running() {
          curl -s http://localhost:8081/jobs/overview 2>/dev/null | \
            jq -e '.jobs[] | select(.name == "insert-into_iceberg_catalog.cybersec.cloudtrail_events" and (.state == "RUNNING" or .state == "RESTARTING" or .state == "CREATED" or .state == "INITIALIZING"))' > /dev/null
        }

        # Function to submit the Java datagen job
        submit_job() {
          echo "Submitting Java CloudTrail DataGen job ($RPS rows/sec)..."
          "$FLINK_BIN" run -d \
            -c com.cloudera.cyber.flink.iceberg.CloudTrailDataGenIcebergJob \
            "$JAR" \
            --catalog.uri http://localhost:8181 \
            --warehouse.name cybersec \
            --s3.endpoint http://localhost:9010 \
            --s3.access-key admin \
            --s3.secret-key admin \
            --rows-per-second "$RPS"
        }

        # Check if job is already running, otherwise submit
        if check_job_running; then
          echo "Java CloudTrail DataGen job is already running. Monitoring..."
        else
          submit_job
        fi

        # Monitor the job and keep process alive
        while true; do
          if ! check_job_running; then
            echo "Job stopped. Resubmitting..."
            submit_job
          fi
          sleep 30
        done
      '';
      process-compose = {
        availability = {
          restart = "on_failure";
          max_restarts = 3;
        };
        depends_on = {
          flink-taskmanager = {
            condition = "process_healthy";
          };
          polaris-init = {
            condition = "process_completed_successfully";
          };
          java-datagen-bootstrap = {
            condition = "process_completed_successfully";
          };
        };
      };
    };

    # Java CloudTrail Iceberg Maintenance
    # Runs the continuous Iceberg Snapshot and Orphan cleanup routine
    java-cloudtrail-maintenance = {
      exec = ''
        echo "Starting Java CloudTrail Iceberg Maintenance job..."

        # Set Flink paths
        export FLINK_HOME="''${FLINK_HOME:-$PWD/thirdparty/flink/flink-dist/target/flink-1.20.1-bin/flink-1.20.1}"
        export FLINK_CONF_DIR="''${FLINK_CONF_DIR:-$DEVENV_STATE/flink/conf}"
        FLINK_BIN="$FLINK_HOME/bin/flink"
        JAR="''${FLINK_COMMON_JAR:-$PWD/flink-cyber/flink-common/target/flink-common-2.4.0.jar}"

        export FLINK_ENV_JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED"

        echo "Submitting Iceberg Maintenance Job..."
        "$FLINK_BIN" run \
          -c com.cloudera.cyber.flink.iceberg.CloudTrailIcebergMaintenanceJob \
          "$JAR"
      '';
      process-compose = {
        availability = {
          restart = "on_failure";
          max_restarts = 3;
        };
        depends_on = {
          flink-taskmanager = {
            condition = "process_healthy";
          };
          polaris-init = {
            condition = "process_completed_successfully";
          };
          java-datagen-bootstrap = {
            condition = "process_completed_successfully";
          };
        };
      };
    };

    # Bootstrap Polaris realm and principal before server starts
    polaris-bootstrap = {
      exec = ''
        POLARIS_HOME="$PWD/thirdparty/polaris/polaris-bin-1.3.0-incubating"

        # Ensure Polaris bin wrapper scripts exist (creates bin/admin and bin/server)
        if [ ! -x "$POLARIS_HOME/bin/admin" ] || [ ! -x "$POLARIS_HOME/bin/server" ]; then
          echo "🔧 Creating Polaris bin wrapper scripts..."
          "$PWD/scripts/setup_polaris_bin.sh" "$POLARIS_HOME"
        fi

        cd "$POLARIS_HOME"

        # Wait for PostgreSQL to be ready (simple TCP check, no psql dependency)
        echo "⏳ Waiting for PostgreSQL to be ready..."
        for i in {1..60}; do
          if nc -z localhost 5438 2>/dev/null || (echo > /dev/tcp/localhost/5438) 2>/dev/null; then
            echo "PostgreSQL port is open"
            break
          fi
          if [ $((i % 10)) -eq 0 ]; then
            echo "   Waiting for PostgreSQL... ''${i}s elapsed"
          fi
          sleep 1
        done
        # Give PostgreSQL a moment to fully initialize after port opens
        sleep 3

        # Configure database connection for bootstrap
        export QUARKUS_DATASOURCE_DB_KIND=postgresql
        export QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://localhost:5438/iceberg?currentSchema=polaris_schema"
        export QUARKUS_DATASOURCE_USERNAME=cybersec
        export QUARKUS_DATASOURCE_PASSWORD=cybersec
        export POLARIS_PERSISTENCE_TYPE=relational-jdbc

        # Try bootstrap - it will fail gracefully if already bootstrapped
        # The admin CLI handles idempotency internally
        # Note: -c= format required (equals sign) per CLI help
        # Note: The -p flag output shows "admin:null" but this is a display bug -
        # the credentials are actually set correctly. Verified by OAuth token request.
        echo "🔧 Bootstrapping Polaris realm..."
        if ./bin/admin bootstrap -v=3 -r=POLARIS -c=POLARIS,admin,admin -p 2>&1 | tee /tmp/polaris-bootstrap.log; then
          echo "Polaris realm bootstrapped successfully"
        else
          # Check if it failed because already bootstrapped (exit code may be non-zero but that's OK)
          if grep -q "already exists\|AlreadyExistsException" /tmp/polaris-bootstrap.log 2>/dev/null; then
            echo "Polaris realm already bootstrapped"
          else
            echo "Bootstrap output:"
            cat /tmp/polaris-bootstrap.log
            echo "Warning: Bootstrap may have failed - server will attempt to start anyway"
          fi
        fi

        echo "Bootstrap complete"
        exit 0
      '';
      process-compose = {
        availability = {
          restart = "no";
        };
        depends_on = {
          postgres = {
            condition = "process_healthy";
          };
        };
      };
    };
    
    polaris = {
      exec = ''
        cd thirdparty/polaris/polaris-bin-1.3.0-incubating
        echo "Starting Apache Polaris REST catalog with PostgreSQL persistence..."
        echo "REST API will be available at http://localhost:8181"
        echo "Admin API will be available at http://localhost:8182"
        
        # Configure AWS SDK for local RustFS (S3-compatible) access
        export AWS_ENDPOINT_URL=http://localhost:9010
        export AWS_REGION=us-east-1
        export AWS_ACCESS_KEY_ID="''${RUSTFS_ACCESS_KEY:-''${MINIO_ACCESS_KEY:-admin}}"
        export AWS_SECRET_ACCESS_KEY="''${RUSTFS_SECRET_KEY:-''${MINIO_SECRET_KEY:-admin}}"
        
        # Quarkus environment variables for PostgreSQL persistence
        export QUARKUS_DATASOURCE_DB_KIND=postgresql
        export QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://localhost:5438/iceberg?currentSchema=polaris_schema"
        export QUARKUS_DATASOURCE_USERNAME=cybersec
        export QUARKUS_DATASOURCE_PASSWORD=cybersec
        export POLARIS_PERSISTENCE_TYPE=relational-jdbc
        
        # AWS SDK v2 properties for S3 endpoint override (RustFS path-style)
        # Bind to 0.0.0.0 for WARP/Cloudflare tunnel access
        export JAVA_TOOL_OPTIONS="-Daws.endpointUrl=http://localhost:9010 -Daws.region=us-east-1 -Daws.s3.pathStyleAccessEnabled=true -Dquarkus.http.host=0.0.0.0 -Dquarkus.config.locations=$PWD/conf/application.properties"
        
        # Run Polaris server
        exec ./bin/server
      '';
      process-compose = {
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 8182;
            path = "/q/health/ready";
          };
          initial_delay_seconds = 15;
          period_seconds = 3;
          failure_threshold = 30;
        };
        depends_on = {
          polaris-bootstrap = {
            condition = "process_completed_successfully";
          };
          postgres = {
            condition = "process_healthy";
          };
          # Warehouse objects live on RustFS — wait for S3 before serving catalog
          rustfs = {
            condition = "process_healthy";
          };
        };
      };
    };

    # Automatic Polaris catalog initialization (cyberphy + RustFS warehouse)
    # Runs after Polaris starts, creates catalog with permissions
    # Uses retry logic and verification for robustness
    polaris-init = {
      exec = ''
        source scripts/polaris_bootstrap_helper.sh
        
        export POLARIS_CATALOG_NAME="''${POLARIS_CATALOG_NAME:-cyberphy}"
        export S3_ENDPOINT="''${S3_ENDPOINT:-http://localhost:9010}"
        export S3_BUCKET="''${S3_BUCKET:-cyberphy}"
        export S3_ACCESS_KEY="''${RUSTFS_ACCESS_KEY:-''${MINIO_ACCESS_KEY:-''${AWS_ACCESS_KEY_ID:-admin}}}"
        export S3_SECRET_KEY="''${RUSTFS_SECRET_KEY:-''${MINIO_SECRET_KEY:-''${AWS_SECRET_ACCESS_KEY:-admin}}}"
        export POLARIS_WAREHOUSE="''${POLARIS_WAREHOUSE:-s3://''${S3_BUCKET}/iceberg/warehouse}"

        # Wait for RustFS before catalog create (warehouse base location)
        log_info "Waiting for RustFS/S3 at $S3_ENDPOINT ..."
        for i in $(seq 1 60); do
          if curl -sf --max-time 2 "$S3_ENDPOINT/health" >/dev/null 2>&1; then
            log_success "RustFS is ready"
            break
          fi
          if [ "$i" -eq 60 ]; then
            log_error "RustFS not ready at $S3_ENDPOINT"
            exit 1
          fi
          sleep 2
        done
        
        # Wait for Polaris to be ready
        if ! wait_for_polaris 60 2; then
          log_error "Polaris did not become ready"
          exit 1
        fi
        
        # Verify bootstrap principal exists
        if ! verify_bootstrap; then
          log_error "Bootstrap principal not found - Polaris may not have bootstrapped correctly"
          exit 1
        fi
        
        # Check if catalog already exists
        if verify_catalog "$POLARIS_CATALOG_NAME"; then
          log_success "Catalog '$POLARIS_CATALOG_NAME' already exists - skipping initialization"
          exit 0
        fi
        
        # Trigger catalog initialization with retry logic
        if trigger_catalog_init 3 ./setup_polaris_catalog.sh; then
          log_success "Catalog initialization completed and verified ($POLARIS_CATALOG_NAME)"
          exit 0
        else
          echo "Failed to initialize Polaris catalog"
          cat /tmp/polaris-catalog-init.log 2>/dev/null || cat /tmp/polaris-init.log 2>/dev/null || true
          exit 1
        fi
      '';
      process-compose = {
        depends_on = {
          polaris = {
            condition = "process_healthy";
          };
          rustfs = {
            condition = "process_healthy";
          };
        };
        # Retry on failure with exponential backoff
        availability = {
          restart = "on_failure";
          max_restarts = 3;
          backoff_seconds = 5;
        };
      };
    };

    # ============================================================================
    # Apache NiFi - Data Flow Visualization
    # ============================================================================
    #
    # Visualizes data pipelines and receives OTEL traces from the collector.
    # ListenOTLP processor receives traces on port 4319.
    #
    # Access: http://localhost:8450/nifi
    # Note: First startup may take 1-2 minutes to initialize.

    nifi = {
      exec = ''
        if [ "''${DISABLE_NIFI:-false}" == "true" ]; then
          echo "NiFi disabled (DISABLE_NIFI=true)"
          sleep infinity
        fi

        echo "======================================================"
        echo "  APACHE NIFI - Data Flow Visualization"
        echo "======================================================"
        echo ""

        # Use thirdparty binary distribution (NiFi 2.0.0)
        NIFI_PACKAGE="$PWD/thirdparty/nifi/nifi-2.0.0"

        if [ ! -d "$NIFI_PACKAGE" ]; then
          echo "NiFi not found at $NIFI_PACKAGE"
          echo "Downloading NiFi 2.0.0..."
          echo ""
          if [ -x "$PWD/scripts/setup_nifi_bin.sh" ]; then
            "$PWD/scripts/setup_nifi_bin.sh" 2.0.0
            if [ ! -d "$NIFI_PACKAGE" ]; then
              echo "ERROR: NiFi download failed"
              exit 1
            fi
          else
            echo "ERROR: Setup script not found at $PWD/scripts/setup_nifi_bin.sh"
            exit 1
          fi
        fi

        # NiFi 2.0 requires running from the package directory for proper JAR loading.
        # NiFi reads config from $NIFI_HOME/conf/nifi.properties, so we modify the
        # package config directly for HTTP-only dev mode.
        NIFI_STATE="$DEVENV_STATE/nifi"

        # Create state directories for NiFi data
        mkdir -p "$NIFI_STATE"/{logs,run,database_repository,flowfile_repository,content_repository,provenance_repository,state,work,extensions}
        mkdir -p "$NIFI_STATE/work/nar"
        mkdir -p "$NIFI_STATE/state/local"
        mkdir -p "$NIFI_STATE/status_repository"
        mkdir -p "$NIFI_STATE/flow_archive"

        # Initialize NiFi configuration on first run
        # We modify the package's nifi.properties directly since NiFi reads from $NIFI_HOME/conf
        NIFI_CONFIGURED_MARKER="$NIFI_STATE/.nifi-configured"
        PROPS="$NIFI_PACKAGE/conf/nifi.properties"
        STATE_XML="$NIFI_PACKAGE/conf/state-management.xml"

        if [ ! -f "$NIFI_CONFIGURED_MARKER" ]; then
          echo "Configuring NiFi for HTTP-only development mode..."

          # Backup original config
          cp "$PROPS" "$PROPS.original" 2>/dev/null || true
          cp "$STATE_XML" "$STATE_XML.original" 2>/dev/null || true

          # Web server - bind to all interfaces on port 8450 (HTTP only)
          # Portable sed in-place: use temp file approach (works on both macOS and Linux)
          sed_inplace() {
            local file="''$1"
            local expr="''$2"
            local tmp="''${file}.tmp.''$$"
            sed "''$expr" "''$file" > "''$tmp" && mv "''$tmp" "''$file"
          }

          sed_inplace "$PROPS" 's|^nifi.web.http.host=.*|nifi.web.http.host=0.0.0.0|'
          sed_inplace "$PROPS" 's|^nifi.web.http.port=.*|nifi.web.http.port=8450|'
          # Clear HTTPS - NiFi requires HTTP OR HTTPS, not both
          sed_inplace "$PROPS" 's|^nifi.web.https.host=.*|nifi.web.https.host=|'
          sed_inplace "$PROPS" 's|^nifi.web.https.port=.*|nifi.web.https.port=|'

          # Clear TLS/security properties for HTTP-only mode
          sed_inplace "$PROPS" 's|^nifi.security.keystore=.*|nifi.security.keystore=|'
          sed_inplace "$PROPS" 's|^nifi.security.keystoreType=.*|nifi.security.keystoreType=|'
          sed_inplace "$PROPS" 's|^nifi.security.keystorePasswd=.*|nifi.security.keystorePasswd=|'
          sed_inplace "$PROPS" 's|^nifi.security.keyPasswd=.*|nifi.security.keyPasswd=|'
          sed_inplace "$PROPS" 's|^nifi.security.truststore=.*|nifi.security.truststore=|'
          sed_inplace "$PROPS" 's|^nifi.security.truststoreType=.*|nifi.security.truststoreType=|'
          sed_inplace "$PROPS" 's|^nifi.security.truststorePasswd=.*|nifi.security.truststorePasswd=|'

          # Disable remote input secure mode
          sed_inplace "$PROPS" 's|^nifi.remote.input.secure=.*|nifi.remote.input.secure=false|'

          # Set sensitive properties key (required)
          sed_inplace "$PROPS" 's|^nifi.sensitive.props.key=.*|nifi.sensitive.props.key=cybersec-dev-key-12345|'

          # Configure paths to use state directory for data persistence
          sed_inplace "$PROPS" "s|^\(nifi.flow.configuration.file=\).*|\1$NIFI_STATE/flow.json.gz|"
          sed_inplace "$PROPS" "s|^\(nifi.flow.configuration.json.file=\).*|\1$NIFI_STATE/flow.json.gz|"
          sed_inplace "$PROPS" "s|^\(nifi.flow.configuration.archive.dir=\).*|\1$NIFI_STATE/flow_archive/|"
          sed_inplace "$PROPS" "s|^\(nifi.database.directory=\).*|\1$NIFI_STATE/database_repository|"
          sed_inplace "$PROPS" "s|^\(nifi.flowfile.repository.directory=\).*|\1$NIFI_STATE/flowfile_repository|"
          sed_inplace "$PROPS" "s|^\(nifi.content.repository.directory.default=\).*|\1$NIFI_STATE/content_repository|"
          sed_inplace "$PROPS" "s|^\(nifi.provenance.repository.directory.default=\).*|\1$NIFI_STATE/provenance_repository|"
          sed_inplace "$PROPS" "s|^\(nifi.state.management.configuration.file=\).*|\1$STATE_XML|"
          sed_inplace "$PROPS" "s|^\(nifi.nar.library.autoload.directory=\).*|\1$NIFI_STATE/extensions|"
          sed_inplace "$PROPS" "s|^\(nifi.nar.working.directory=\).*|\1$NIFI_STATE/work/nar/|"
          sed_inplace "$PROPS" "s|^\(nifi.documentation.working.directory=\).*|\1$NIFI_STATE/work/docs/components|"
          sed_inplace "$PROPS" "s|^\(nifi.status.repository.questdb.persist.location=\).*|\1$NIFI_STATE/status_repository|"

          # Update state-management.xml with absolute path for local state
          sed_inplace "$STATE_XML" "s|<property name=\"Directory\">./state/local</property>|<property name=\"Directory\">$NIFI_STATE/state/local</property>|g"

          touch "$NIFI_CONFIGURED_MARKER"
          echo "NiFi configured for development mode."
        fi

        echo ""
        echo "Starting NiFi on port 8450..."
        echo "  Web UI:      http://localhost:8450/nifi"
        echo "  OTLP Port:   4319 (for ListenOTLP processor)"
        echo "  Package:     $NIFI_PACKAGE"
        echo "  Data:        $NIFI_STATE"
        echo ""
        echo "Note: First startup may take 1-2 minutes to initialize."
        echo "      Check logs/nifi-user.log for single-user credentials on first run."
        echo ""

        # Set environment - NiFi requires NIFI_OVERRIDE_NIFIENV=true to respect env vars
        export NIFI_OVERRIDE_NIFIENV="true"
        export NIFI_HOME="$NIFI_PACKAGE"
        export NIFI_LOG_DIR="$NIFI_STATE/logs"
        export NIFI_PID_DIR="$NIFI_STATE/run"

        # MinIO/S3 credentials (for NiFi S3 processors)
        export AWS_ACCESS_KEY_ID="admin"
        export AWS_SECRET_ACCESS_KEY="admin"
        export AWS_ENDPOINT_URL="http://localhost:9010"

        cd "$NIFI_PACKAGE"

        # Run NiFi in foreground mode
        exec "$NIFI_PACKAGE/bin/nifi.sh" run
      '';
      process-compose = {
        depends_on = {
          postgres = {
            condition = "process_healthy";
          };
        };
        readiness_probe = {
          http_get = {
            host = "localhost";
            port = 8450;
            path = "/nifi-api/system-diagnostics";
          };
          initial_delay_seconds = 60;
          period_seconds = 5;
          failure_threshold = 24;
        };
        # Auto-downloads if missing. Disable with DISABLE_NIFI=true (not recommended)
      };
    };
  };

  # Initialize Iceberg catalog in PostgreSQL
  scripts.init-iceberg.exec = ''
    echo "Initializing Iceberg catalog in PostgreSQL..."
    PGPASSWORD=cybersec psql -h localhost -p 5438 -U cybersec -d iceberg -c "
      CREATE SCHEMA IF NOT EXISTS iceberg;
      CREATE TABLE IF NOT EXISTS iceberg.catalog_tables (
        catalog_name VARCHAR(255) NOT NULL,
        table_namespace VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        metadata_location VARCHAR(1024),
        previous_metadata_location VARCHAR(1024),
        PRIMARY KEY (catalog_name, table_namespace, table_name)
      );
    " || echo "Iceberg catalog schema already exists"
  '';
   
  # See full reference at https://devenv.sh/reference/options/
}


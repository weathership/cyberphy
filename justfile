# cybersec — task runner (migrating from `devenv tasks` to `just`)
# Recipes run under bash; each line is its own shell, so a failing step halts the recipe.
set shell := ["bash", "-euo", "pipefail", "-c"]

_root    := justfile_directory()
_sb      := _root / "infra/aws/tofu-sandbox/run-sandbox.sh"
_hydrate := "uv run python " + (_root / "infra/aws/tofu-sandbox/hydrate.py")

# List available recipes
default:
    @just --list

# ----------------------------------------------------------------------------
# Air-gap convergence SANDBOX — throwaway AWS rehearsal of the resilient deploy.
# Config: config/reference.conf `cybersec.sandbox` (override per-dev via SANDBOX_* env).
# Per-dev state (tofu state, generated key, /32) -> build/sandbox (gitignored).
# Full guide: infra/aws/tofu-sandbox/RUNBOOK.md
# ----------------------------------------------------------------------------

# Hydrate HOCON cybersec.sandbox -> build/sandbox/{tfvars,env}; auto-detect /32
sandbox-config:
    {{_hydrate}}

# Full rehearsal: provision -> transport -> cut egress -> converge -> verify
sandbox: sandbox-config
    {{_sb}} up
    {{_sb}} transport
    {{_sb}} airgap
    {{_sb}} converge
    {{_sb}} verify

# Provision a fresh node (egress ON) and wait for RKE2 Ready
sandbox-up: sandbox-config
    {{_sb}} up

# scp packages + stage engine + deploy MinIO (egress ON; fetches zarf-init if missing)
sandbox-transport:
    {{_sb}} transport

# Cut egress -> closed world (confirms outbound is blocked)
sandbox-airgap:
    {{_sb}} airgap

# Restore egress
sandbox-online:
    {{_sb}} online

# The one-command resilient deploy (S3 creds kept off argv)
sandbox-converge:
    {{_sb}} converge

# converge --verify + pod / agent / ingress check
sandbox-verify:
    {{_sb}} verify

# Clean-slate the app stack (registry + node images conserved)
sandbox-teardown:
    {{_sb}} teardown

# Tear the sandbox down (stops the meter)
sandbox-destroy:
    {{_sb}} destroy

# ssh to the node, optionally running a command: just sandbox-ssh "kubectl get pods -A"
sandbox-ssh *args:
    {{_sb}} ssh {{args}}

# Bootstrap status + node readiness
sandbox-status:
    {{_sb}} status

# Print the node public IP
sandbox-ip:
    {{_sb}} ip

# One-command end-to-end FSM validation: provision → transport → air-gap → induce each
# wedged registry/storage state (default-SC capture, Released PV, class-drift, …) →
# converge → assert recovery → destroy. `--keep` skips the destroy (auto-kept on failure).
sandbox-test-fsm *args: sandbox-config
    bash {{_root}}/infra/aws/tofu-sandbox/test-fsm.sh {{args}}

# ----------------------------------------------------------------------------
# cybersec-dask IMAGE / PACKAGE / REDEPLOY (Zarf wire name; product = Cyberphy)
# content-tagged, registry-pushed,
# converge-drift-aware. Replaces the devenv zarf:image / zarf:package tasks; a
# content-derived tag means every image change is a new tag the drift detect rolls.
# ----------------------------------------------------------------------------
_ops := _root / "zarf/scripts/ops.sh"

# Print the content-derived image tag (BASE-<hash of the image build inputs>)
image-tag:
    @bash {{_ops}} tag

# Build the image (content tag), bump the tag in the manifests, push to the local registry
image:
    bash {{_ops}} image

# Create the Zarf package (pulls the fresh image from the local registry) + closure gate
package:
    bash {{_ops}} package

# Redeploy to the live AWS cluster (package transport + drift-aware converge apply)
redeploy:
    bash {{_ops}} redeploy

# EXPERIMENTAL fast redeploy (image-delta push) — WIP: podman-VM tunnel obstacle; see ops.sh
redeploy-fast:
    bash {{_ops}} redeploy-fast

# ----------------------------------------------------------------------------
# AIR-GAP RELEASE — assemble the complete convergent-deploy bundle for GitHub.
# ----------------------------------------------------------------------------

# Assemble build/release/: convergence-engine tarball + zarf-init pkg + zarf binary +
# runbook + SHA256SUMS (the 1.3 GB deploy package is referenced in place at zarf/).
# Then cut the GitHub release from those + the deploy package.
release-bundle:
    bash {{_root}}/zarf/scripts/release-bundle.sh

# ----------------------------------------------------------------------------
# LAPTOP DEV — zarf-style stack on a local k3d cluster with Tilt live-reload.
# The "laptop local" (k3d), distinct from the "workstation local" (RKE2 on a GPU
# box) and the air-gap Zarf deploy. Manifest-direct (no `zarf init`); reuses the
# devenv MinIO (run `devenv up` first). Guide: docs/.../laptop-dev/k3d-tilt.md
# ----------------------------------------------------------------------------
_dev := "bash " + (_root / "scripts/dev-k3d.sh")

# Stand up the whole zarf-style stack on k3d + Tilt (cluster->image->deploy->seed->tilt up)
dev-up:
    {{_dev}} up

# Create the k3d cluster + managed registry (idempotent)
dev-cluster:
    {{_dev}} cluster

# Build the cybersec-dask image + import it into k3d (Dask baseline; one-time/slow)
dev-image:
    {{_dev}} image

# Apply the base resources (namespaces, config/secret, Dask operator + cluster, services)
dev-deploy:
    {{_dev}} deploy

# Seed OTEL parquet (mode=minimal) into the devenv MinIO bucket
dev-seed:
    {{_dev}} seed

# Start Tilt against the running k3d cluster (live-reload the two app Deployments)
dev-tilt:
    {{_dev}} tilt

# Pods/services across the dev namespaces
dev-status:
    {{_dev}} status

# Delete the k3d cluster + registry (devenv MinIO + its data are left intact)
dev-down:
    {{_dev}} down

# Lab env + hub probes + bootstrap-check

## Done
- `scripts/lab_env.sh`: shared KUBECONFIG/RKE2 resolver, RustFS health, NodePort probes, `find_zarf_package` (prefers mirror 1.6.5)
- Wired into: enterShell, `lab:status`, `k8s:status`, `zarf:local:{preflight,init,deploy,deploy-dashboard,status}`
- Hub UI: MinIO label → RustFS; `/api/services/health` live probes; green/red status dots on service links (index + settings)
- Catalog defaults: cyberphy warehouse, admin/admin (RustFS), env-overridable
- bootstrap-check: RustFS, Polaris cyberphy catalog, KUBECONFIG/Zarf/package readiness
- Bootstrap `_check_minio`: probes RustFS `/health` first; display message "RustFS is healthy"

## Verified
- `find_zarf_package` → `build/cyberphy-release-mirror/1.6.5/zarf-package-cybersec-dask-amd64-1.6.5.tar.zst`
- Live probes: Jupyter :30080, Dask :30087, Viz :30506, RustFS :9010/:9011 all up

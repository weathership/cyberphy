# Cyberphy

**Cyber-physical systems observability and analytics** — OpenTelemetry from the plant floor and robot cell, through stream processing, into a lakehouse you can query and explore.

This repository ships:

1. **Air-gap Kubernetes releases** ([`zarf/`](zarf/)) — primary product delivery  
2. **AWS / on-prem infrastructure** ([`infra/`](infra/)) — OpenTofu + Ansible for RKE2 and the same stack  
3. The **processing and UI stack** (Flink, NiFi, Polaris, Iceberg, Dask, Panel) that turns CPS and system OTel into durable, queryable data  

Legacy Cloudera Manager packaging (parcel / CSD) is **not** part of this project. Upstream cybersecurity history remains on the `upstream` / `rch` remotes if needed; **this** tree targets [weathership/cyberphy](https://github.com/weathership/cyberphy).

---

## Vision

Cyber-physical systems (manufacturing lines, robotics cells, industrial controllers) and the software that runs them all emit telemetry. We treat that telemetry as first-class **OpenTelemetry** data:

| Source | Examples | Path |
|--------|----------|------|
| **CPS / edge** | robot joint streams, PLC/SCADA events, cell controllers, machine vision jobs | ingest → normalize → stream → lakehouse |
| **The platform itself** | Flink jobs, NiFi flows, Dask workers, Panel/engine, K8s | OTel metrics/traces/logs back into the same lake |

Downstream:

- **Flink** (and friends) process event streams that look like manufacturing and robotics messages—not only classical security logs.  
- **Those processors emit OTel** about their own work (lag, enrichment, failures).  
- **Iceberg** (via Polaris / MinIO or S3) holds long-lived span and event tables.  
- **Dask + Panel** explore multi-partition OTel at interactive scale (air-gap capable).

```text
  CPS devices / robots / PLCs          Platform (Flink, NiFi, Dask, UI)
           │                                        │
           │  OTel / domain events                  │  OTel self-telemetry
           ▼                                        ▼
     ┌─────────────────────────────────────────────────────┐
     │  Ingest (NiFi / connectors) → Flink pipelines       │
     │  Catalog: Polaris  ·  Tables: Iceberg  ·  Obj: S3   │
     └───────────────────────┬─────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
     Interactive (Panel / Jupyter)    Batch / SQL analytics
              │
              └── Air-gap K8s via Zarf release (see zarf/)
```

---

## Primary delivery: `zarf/`

**Zarf packages** are the supported way to put the interactive stack on **air-gapped RKE2** (and related K8s):

| Piece | Role |
|-------|------|
| `zarf/zarf.yaml` | Package definition (Dask, JupyterHub, Panel-Viz, images) |
| `zarf/converge/` | Idempotent deploy/verify engine (detect → rem → fixpoint) |
| `zarf/scripts/converge-node.sh` | Node entrypoint (no package required for config-only rem) |
| `zarf/scripts/verify-s3-datapath.sh` | Prove configured S3 + marker + span parquet |
| `zarf/AIRGAP-*.md` | Discovery, remediations, runbooks |

**Start here:**

```bash
# On an RKE2 control plane (package already staged, or config-only with S3 creds):
sudo bash zarf/scripts/converge-node.sh verify
sudo env CONVERGE_CREDS_FILE=/dev/shm/s3-creds bash zarf/scripts/converge-node.sh apply
sudo bash zarf/scripts/verify-s3-datapath.sh
```

Full package docs, variables, and ports: **[`zarf/README.md`](zarf/README.md)** · **[`zarf/RUNBOOK.md`](zarf/RUNBOOK.md)** · **[`zarf/AIRGAP-CONVERGE-RUNBOOK.md`](zarf/AIRGAP-CONVERGE-RUNBOOK.md)**

---

## Infrastructure: `infra/`

**AWS and lab K8s** for the same platform—not Cloudera Manager.

| Path | Role |
|------|------|
| [`infra/aws/tofu/`](infra/aws/tofu/) | OpenTofu: VPC, RKE2 nodes, S3, security groups |
| [`infra/aws/ansible/`](infra/aws/ansible/) | RKE2, Zarf stage/deploy, Dask/Jupyter/Panel playbooks |
| [`infra/aws/tofu-sandbox/`](infra/aws/tofu-sandbox/) | Smaller / FSM test sandbox |
| [`infra/LOCAL.md`](infra/LOCAL.md) | Local k3d / existing RKE2 paths |

**Typical AWS path:**

```bash
# From a developer machine with devenv / tofu / ansible configured:
devenv tasks run aws:provision    # or tofu apply under infra/aws/tofu
devenv tasks run aws:deploy       # Ansible → RKE2 + Zarf stack
# Then air-gap-style verify on the node via zarf/scripts/converge-*.sh
```

Overview and mode matrix: **[`infra/README.md`](infra/README.md)** · AWS detail: **[`infra/aws/README.md`](infra/aws/README.md)**

---

## Platform stack (stays)

Everything below remains first-class. Domain payloads shift toward **CPS + platform OTel**; the engines stay.

| Layer | Components |
|-------|------------|
| **Stream / flow** | Apache Flink (`flink-cyber/` pipeline toolkit, `thirdparty/flink`), Apache NiFi (`thirdparty/nifi`) |
| **Table / catalog** | Apache Iceberg (`thirdparty/iceberg`), Apache Polaris (`thirdparty/polaris`), S3/RustFS |
| **Interactive** | Dask, JupyterHub, Panel OTEL Navigator / Data-View, Navigator engine + PTY |
| **Local lab** | `devenv` (Postgres, Polaris bootstrap, RustFS/S3, Flink UI, observability ports) |
| **Python ops** | Import path `cybersec/` (transitional); CLI **`cyberphy`** / **`cyberphy-mcp`** (aliases: `cybersec`, `cybersec-mcp`) |

Build Flink toolkit (no CM packaging):

```bash
cd flink-cyber
mvn clean install -DskipTests
# cyber-parcel / cyber-csd removed from the reactor — not built
```

Local core services:

```bash
devenv up
# Flink :8081 · Iceberg browser :5050 · RustFS (S3) :9010/:9011 · Polaris :8181 · …
```

---

## Repository map

```text
zarf/           # ★ Zarf releases, converge, air-gap runbooks  (primary deliverable)
infra/          # ★ OpenTofu AWS + Ansible RKE2 / stack deploy
flink-cyber/    # Flink pipelines (parse, enrich, index, profile) — no CM parcel/CSD
thirdparty/     # flink/iceberg/nifi/polaris gitlinks (HTTPS); flink-python vendored PyFlink
cybersec/       # Python ops + engine (import path; product CLI is cyberphy)
docs/           # Deeper ops / architecture notes
```

---

## What we are not doing

- **Cloudera Manager** parcels, CSDs, or CDP-centric install paths  
- Positioning this tree as a pure “security SIEM” product — telemetry is **OTel from CPS and from the platform**  
- Requiring a full monorepo rebuild to ship a **Zarf** release — zarf image + package is the release unit for the interactive stack  

---

## Naming (cyberphy vs cybersec)

| Layer | Name | Notes |
|-------|------|--------|
| **Product / docs / CLI** | **cyberphy** | Brand, book, `cyberphy` / `cyberphy-mcp` entrypoints |
| **Python import path** | `cybersec.*` | Transitional; full package rename later |
| **CLI aliases** | `cybersec`, `cybersec-mcp` | Same code as cyberphy |
| **Zarf package / image** | `cybersec-dask` | Air-gap drop-in; release assets keep this id |
| **K8s cluster resource** | `cybersec-dask` DaskCluster | Field deploys already use this name |
| **Local buckets** | `cyberphy`, `cyberphy-hx` | RustFS at `/raid/build/cyberphy/data/` |
| **Postgres DB** | `cybersec` (name transitional) | Local catalog DB |
| **Git remotes** | see below | `origin` is weathership/**cyberphy** |

## Remotes

| Remote | URL | Role |
|--------|-----|------|
| `origin` | `git@github.com:weathership/cyberphy.git` | This project |
| `upstream` | `git@github.com:cloudera/cybersec.git` | Historical cybersecurity upstream |
| `rch` | `git@github.com:rch/cldr-cybersec.git` | Personal fork / prior work |

---

## Documentation

mdBook sources live under [`docs/current/`](docs/current/) (same layout as Ægir: curated book + `docs/scratch/` notes).

- **Local build:** `cd docs/current && mdbook build` (optional: D2 + `mdbook-d2` for diagrams)  
- **CI:** [`.github/workflows/docs.yml`](.github/workflows/docs.yml) builds and deploys **GitHub Pages** on push to **`trunk`** when `docs/**` changes  
- **Published site:** enable Pages for the `weathership/cyberphy` repo (source: GitHub Actions)

---

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE).

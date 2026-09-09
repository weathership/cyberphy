# Workload Overview

The Cyberphy Toolkit supports three primary workload patterns, each representing how security teams actually work day-to-day.

## Workload Patterns

```d2
direction: right

Laptop: {
  label: "Laptop Development"
  style.fill: "#e8f5e9"

  flink: "Local Flink"
  k3d: "K3d (optional)"
  devenv: "devenv services"
}

Workstation: {
  label: "Workstation + GPUs"
  style.fill: "#fff3e0"

  flink: "Flink Cluster"
  cyber: "Cyber Toolkit"
  rke2: "RKE2 System"
  gpu: "GPU Compute"
}

Benchmark: {
  label: "Benchmarking"
  style.fill: "#e3f2fd"

  local: "Local Flink\n+ Telemetry"
  remote: "Remote RKE2\non AWS"
  dask: "Dask + Jupyter"
}

Laptop -> Workstation: "Scale up"
Workstation -> Benchmark: "Multi-cluster"
```

## Choosing Your Workload

| I want to... | Workload | Start here |
|--------------|----------|------------|
| Develop and test pipelines | Laptop Development | [laptop-dev.md](./laptop-dev.md) |
| Run security analysis with ML | Workstation + GPUs | [workstation.md](./workstation.md) |
| Benchmark across environments | Multi-Cluster | [benchmarking.md](./benchmarking.md) |

## Common Elements

All workloads share:

- **Iceberg tables** for data storage
- **Polaris REST catalog** for metadata
- **Health diagnostics** via `cybersec health`
- **Bootstrap system** for environment setup

## Behavior-Driven Scenarios

Each workload chapter includes a **Scenarios** section that maps to Gherkin feature files in the `features/` directory. These scenarios define:

- **Given**: Initial environment state
- **When**: User actions
- **Then**: Expected outcomes

This ensures documentation stays grounded in testable, real-world workflows.

## Environment Progression

Most users follow a natural progression:

1. **Start on laptop**: Develop pipelines, test locally
2. **Move to workstation**: Add GPU acceleration, full RKE2
3. **Scale to benchmarking**: Multi-cluster telemetry, Dask analytics

Each transition is designed to be incremental—the same code runs across all environments.

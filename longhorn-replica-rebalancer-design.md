# Longhorn Replica Rebalancer Design

## Problem

When a homeserver goes down, Longhorn replicates volumes to the 2 remaining homeservers (1 replica per node). When the 3rd homeserver comes back, the distribution is 1-1-0 per volume. Longhorn's `replicaAutoBalance: best-effort` does not trigger a rebalance because it considers this distribution balanced (no node has more than 1 replica for any given volume).

The manual workaround is to open the Longhorn UI, identify volumes where the returning node has no replica, and delete one replica from a node that has "too many" across all volumes. Longhorn then rebuilds the deleted replica on the returning node. This takes ~10-12 manual replica deletions spread across 2 nodes.

## Goal

Automate the manual rebalancing process with a Python CronJob that runs inside the k3s cluster, processes one replica at a time, and is safe by default (dry-run enabled).

## Approach Chosen

Python CronJob using the Kubernetes API to manipulate Longhorn CRDs (`volumes.longhorn.io`, `replicas.longhorn.io`, `nodes.longhorn.io`). The Kubernetes API is more stable than Longhorn's REST API across upgrades, RBAC is standard, and deleting Replica CRs triggers the same behavior as the UI deletion.

### Alternatives Considered

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| Longhorn REST API | Purpose-built, clean JSON | Unversioned, can break on upgrades | Rejected |
| Kubernetes CRD API | Stable interface, standard RBAC, well-documented client | Slightly more RBAC setup | **Selected** |
| 3 replicas instead of 2 | Zero code, native Longhorn | 50% more storage, homeserver3 (NVME only) fills faster | Not feasible for storage capacity |

## Architecture

```
CronJob (schedule: every 6h)
  Pod (python:3.12-slim image)
    rebalancer script
      Kubernetes API (via in-cluster config)
        Read: volumes.longhorn.io, replicas.longhorn.io, nodes.longhorn.io
        Write: delete replicas.longhorn.io (only mutation)
      Configurable via env vars
```

## Algorithm

1. **Discover storage nodes**: list `nodes.longhorn.io`, filter to nodes with `allowScheduling: true` (the 3 homeservers).
2. **List all volumes**: filter to attached, healthy volumes only. Skip degraded, faulted, detached, or rebuilding volumes.
3. **Map replica placement**: for each volume, record which node each running replica lives on.
4. **Identify imbalanced volumes**: volumes where a storage node has 0 replicas while others have 1+. These are candidates for rebalancing.
5. **Score nodes**: count total replicas per node across all volumes. The node with the highest count is the "donor".
6. **Select one volume**: pick an imbalanced volume where deleting a replica from the donor node would improve the overall balance.
7. **Delete the replica**: delete the Replica CR from the donor node.
8. **Wait for healthy**: poll the volume until it returns to healthy state (replica rebuilt on the underloaded node).
9. **Repeat**: up to `MAX_REBALANCES_PER_RUN` (default: 1), then exit.

## Safety Guards

| Guard | Behavior |
|-------|----------|
| Volume not healthy | Skip: never touch a degraded volume |
| Volume detached | Skip: cannot rebuild without an engine |
| Volume already rebuilding | Skip: do not stack rebuilds |
| Running replicas != desired count | Skip: only delete when the volume is fully healthy with all expected replicas running |
| Max rebalances per run | Default 1, configurable. Prevents IO storms |
| Wait timeout | If rebuild does not complete within timeout (default 30min), exit with error. Do not proceed to next volume |
| Dry-run mode | Log what would happen without deleting anything. Enabled by default |
| No imbalanced volumes found | Exit 0 cleanly |

## Configuration

All configuration is via environment variables on the CronJob.

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_REBALANCES_PER_RUN` | `1` | Max replica deletions per execution |
| `REBUILD_TIMEOUT_SECONDS` | `1800` | Max wait time for rebuild completion |
| `POLL_INTERVAL_SECONDS` | `30` | Interval between rebuild status checks |
| `DRY_RUN` | `true` | Safe default: must explicitly set to `false` to enable deletions |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `LONGHORN_NAMESPACE` | `longhorn-system` | Namespace for Longhorn resources |

## Repository & Packaging

The rebalancer lives in its own repository (`longhorn-replica-rebalancer`), separate from the GitOps repo. This keeps the GitOps repo focused on cluster state declarations.

| Concern | Location |
|---------|----------|
| Python code, tests, Dockerfile | `longhorn-replica-rebalancer` repo |
| CI: run tests, build image, push to Nexus | GitHub Actions in that repo |
| CronJob + RBAC manifests | `home-cluster-gitops/resources/longhorn/` |

### Container Image

- Built from a `Dockerfile` in the rebalancer repo
- Pushed to the Nexus container registry
- Image tag pinned in the GitOps CronJob manifest, updated by Renovate

### CI Pipeline (GitHub Actions)

1. Run `pytest` (unit tests)
2. Run linting (`ruff check`, `ruff format --check`)
3. Build Docker image
4. Push to Nexus registry with version tag and `latest`

### Project Structure (external repo)

```
longhorn-replica-rebalancer/
  src/
    rebalancer/
      __init__.py
      main.py          # Entry point, orchestration loop
      discovery.py     # Node/volume/replica discovery from K8s API
      balancer.py      # Imbalance detection and donor/volume selection
      executor.py      # Replica deletion and rebuild wait logic
  tests/
    conftest.py        # Shared fixtures (mock K8s responses)
    test_discovery.py
    test_balancer.py
    test_executor.py
    test_main.py       # Integration tests for the orchestration loop
  Dockerfile
  pyproject.toml
  .github/
    workflows/
      ci.yml           # Test, lint, build, push
```

## Testing Strategy

Unit tests for each module with mocked Kubernetes API responses. Fixtures model real Longhorn CRD shapes (Volume, Replica, Node).

Scenario coverage:
- Balanced cluster: no-op, exits cleanly
- Single imbalanced volume: one replica deleted and rebuilt
- Multiple imbalanced volumes with `MAX_REBALANCES_PER_RUN=1`: only one processed
- Degraded volume: skipped
- Detached volume: skipped
- Rebuild timeout: exits with error
- Dry-run mode: logs actions without mutations

Test runner: `pytest` with no external dependencies (mocked K8s client).

## Kubernetes Resources (GitOps-managed, in home-cluster-gitops)

All resources deployed via ArgoCD alongside existing Longhorn resources. These manifests live in the GitOps repo under `resources/longhorn/` and reference the container image from Nexus.

- **CronJob**: schedule `0 */6 * * *` (every 6 hours), `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 3`, image from Nexus registry
- **ServiceAccount**: dedicated account for the rebalancer pod
- **ClusterRole**: read access to `volumes.longhorn.io`, `replicas.longhorn.io`, `nodes.longhorn.io`; delete access to `replicas.longhorn.io` only
- **ClusterRoleBinding**: binds the ClusterRole to the ServiceAccount

## Constraints

- Does not `kubectl apply`, `kubectl patch`, or `kubectl edit` anything: respects GitOps discipline
- Does not change volume settings or replica counts
- Does not touch replicas of unhealthy volumes
- Does not run multiple rebalances concurrently (CronJob `concurrencyPolicy: Forbid`)
- Processes one replica at a time, waits for rebuild, then optionally continues

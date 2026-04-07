# longhorn-replica-rebalancer

> **Homelab project.** This tool was built for a personal homelab cluster used as a playground for testing infrastructure, Kubernetes, and storage patterns. It may be unstable and is not designed for production use. Use at your own risk.

Automated Longhorn volume replica rebalancer for Kubernetes clusters. Detects imbalanced replica distribution across storage nodes and safely migrates replicas to achieve even spread, preventing replica storms during node failures.

## Problem

When a Longhorn storage node goes offline and comes back, its replicas are rebuilt on the remaining nodes. This creates an imbalanced distribution (e.g., 16/0/16 across 3 nodes). If one of the overloaded nodes then fails, all 16 replicas need to rebuild simultaneously, causing an I/O storm.

This tool gradually rebalances replicas so that the distribution approaches an even split (e.g., 11/11/10), reducing the blast radius of any single node failure.

## How It Works

1. Discovers storage nodes with scheduling enabled
2. Checks cluster health (refuses to run if any volume is faulted, degraded, or rebuilding)
3. Lists all healthy, attached volumes and maps their replica placement
4. Identifies imbalanced volumes (volumes missing replicas on nodes that have fewer total replicas)
5. Selects the smallest imbalanced volume from the most-loaded node (smaller volumes rebuild faster)
6. Deletes one replica from the donor node (Longhorn automatically schedules a new replica on the least-loaded node)
7. Waits for the volume to return to healthy state before proceeding
8. Repeats up to `MAX_REBALANCES_PER_RUN` times, re-checking cluster health between each operation

## Safety

- **Dry-run by default** (`DRY_RUN=true`): logs what it would do without making changes
- **Cluster health pre-flight**: refuses to run if any storage node is not ready, or any volume is faulted/degraded/rebuilding
- **Health re-check between iterations**: stops mid-run if the cluster degrades
- **One replica at a time**: waits for full rebuild before touching the next volume
- **Smallest volumes first**: minimizes rebuild time and I/O impact
- **Rebuild timeout**: aborts if a volume doesn't become healthy within the timeout period
- **No overlap**: CronJob `concurrencyPolicy: Forbid` prevents parallel runs

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DRY_RUN` | `true` | Set to `false` to enable actual replica deletion |
| `MAX_REBALANCES_PER_RUN` | `1` | Maximum number of replicas to move per run |
| `REBUILD_TIMEOUT_SECONDS` | `1800` | How long to wait for a volume to become healthy after deleting a replica (30 min) |
| `POLL_INTERVAL_SECONDS` | `30` | How often to check volume health during rebuild |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LONGHORN_NAMESPACE` | `longhorn-system` | Kubernetes namespace where Longhorn is installed |

## Deployment

The rebalancer runs as a Kubernetes CronJob. The deployment manifests (CronJob, ServiceAccount, ClusterRole, ClusterRoleBinding) are managed in [home-cluster-gitops](https://github.com/gillouche/home-cluster-gitops) and deployed via ArgoCD.

### Required RBAC Permissions

```yaml
rules:
- apiGroups: ["longhorn.io"]
  resources: ["nodes", "volumes", "replicas"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["longhorn.io"]
  resources: ["replicas"]
  verbs: ["delete"]
```

### Example Log Output

```
Replica distribution: node-0=16, node-1=0, node-2=16
Volumes: 16 total, 16 imbalanced
Imbalanced volume sizes: smallest=pvc-029b... (5GB), largest=pvc-bce8... (200GB)
Will rebalance up to 1 volume(s) this run (smallest volumes first)
Selected volume=pvc-029b... (5GB), donor=node-2 (count=16), replica=pvc-029b...-r-0ecc
Rebalance 1/1: deleting replica pvc-029b...-r-0ecc from node node-2 (volume pvc-029b...)
Deleting replica pvc-029b...-r-0ecc
Replica pvc-029b...-r-0ecc deleted
Waiting for volume pvc-029b... to become healthy (timeout=1800s)
Volume pvc-029b...: state=attached, robustness=degraded, waiting...
Volume pvc-029b... is healthy
Completed 1 rebalance(s)
```

## Development

### Prerequisites

- [Nix](https://nixos.org/) with flakes enabled (provides Python 3.14, uv, pre-commit)
- Or manually: Python 3.14+, [uv](https://github.com/astral-sh/uv)

### Setup

```bash
# With Nix (recommended)
direnv allow  # or: nix develop

# Without Nix
uv venv
uv sync --all-extras
source .venv/bin/activate
pre-commit install
```

### Running Tests

```bash
uv run pytest -v --cov=rebalancer --cov-report=term-missing
```

### Linting

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
```

### Running Locally (Dry-Run)

Requires a kubeconfig with access to a cluster running Longhorn:

```bash
uv run python -m rebalancer.main
```

## Project Structure

```
src/rebalancer/
  main.py        - Entry point, orchestration loop, cluster health checks
  discovery.py   - Kubernetes API queries for Longhorn nodes, volumes, replicas
  balancer.py    - Imbalance detection, size-aware donor selection algorithm
  executor.py    - Replica deletion and rebuild polling
tests/
  conftest.py    - Test fixtures and factory functions
  test_main.py   - Integration tests for the orchestration loop
  test_balancer.py - Unit tests for balancing algorithm (parametrized for 3 and 5 nodes)
  test_discovery.py - Unit tests for Kubernetes API queries
  test_executor.py  - Unit tests for replica deletion and health polling
```

## License

MIT

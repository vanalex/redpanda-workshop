# Redpanda HA on Kubernetes

Deploy a highly available 3-broker Redpanda cluster on Kubernetes, then test it against node maintenance, quorum loss, and permanent node failure.

## What you'll learn

- How Redpanda's per-partition Raft replication differs from Kafka's ISR model
- Why RF=3 tolerates exactly one broker loss (and what happens at two)
- Using maintenance mode for zero-impact node drains
- How PodDisruptionBudgets and topology spread constraints protect quorum
- Recovering from permanent node loss with broker decommissioning

## Prerequisites

- [k3d](https://k3d.io) (or any 3-node Kubernetes cluster spanning 3 zones)
- [Helm](https://helm.sh) v3+
- kubectl

## Architecture

```
zone-a                zone-b                zone-c
┌────────────┐        ┌────────────┐        ┌────────────┐
│ redpanda-0 │◄──────►│ redpanda-1 │◄──────►│ redpanda-2 │
│  (PVC 0)   │  Raft  │  (PVC 1)   │  Raft  │  (PVC 2)   │
└────────────┘        └────────────┘        └────────────┘
```

- One Raft group **per partition** (plus a controller group for metadata) — no ZooKeeper, no separate controller quorum, no JVM
- `acks=all` commits on a Raft **majority** (2 of 3). There is no `min.insync.replicas`
- Replication factors must be **odd**: RF=3 tolerates 1 failure, RF=5 tolerates 2
- Lose 1 broker → leaders re-elect, traffic continues. Lose 2 → affected partitions go leaderless and block until quorum returns. No unclean-leader-election knob to trade consistency away

## Quick start

### 1. Create the Kubernetes cluster

```bash
k3d cluster create rp-cluster \
  --agents 3 \
  --k3s-node-label topology.kubernetes.io/zone=zone-a@agent:0 \
  --k3s-node-label topology.kubernetes.io/zone=zone-b@agent:1 \
  --k3s-node-label topology.kubernetes.io/zone=zone-c@agent:2

kubectl get nodes -L topology.kubernetes.io/zone
```

### 2. Install Redpanda

```bash
helm repo add redpanda https://charts.redpanda.com
helm repo update

helm install redpanda redpanda/redpanda \
  --namespace redpanda --create-namespace \
  --set statefulset.replicas=3 \
  --set external.enabled=false \
  --set tls.enabled=false \
  --set resources.cpu.cores=1 \
  --set resources.memory.container.max=2Gi
```

> ⚠️ TLS and external access are disabled to keep the walkthrough focused on availability. Don't do this in production.

The chart gives you the HA building blocks out of the box:

| Resource | Purpose |
|---|---|
| StatefulSet | Stable pod identity (`redpanda-0/1/2`) and per-broker PVCs |
| Headless Service | Stable DNS per broker (`redpanda-0.redpanda.redpanda.svc...`) |
| PodDisruptionBudget | `maxUnavailable: 1` — voluntary disruptions can never break quorum |
| Topology spread constraints | One broker per zone/node |

### 3. Verify

```bash
kubectl -n redpanda get pods -o wide     # one broker per node
kubectl -n redpanda get pdb              # maxUnavailable: 1

kubectl -n redpanda exec -it redpanda-0 -c redpanda -- rpk cluster health
```

Expected:

```
Healthy:                          true
All nodes:                        [0 1 2]
Leaderless partitions (0):        []
Under-replicated partitions (0):  []
```

### 4. Create a topic and produce/consume

`rpk` ships inside every broker container — no client pod needed.

```bash
# Create topic with replication factor 3
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic create test -r 3 -p 1

# Produce (Ctrl+C to exit)
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic produce test

# Consume
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic consume test --offset start

# Where does the partition live?
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic describe test -p
```

```
PARTITION  LEADER  REPLICAS
0          1       [0 1 2]
```

## Failure scenarios

### Scenario 1 — Drain the node hosting the leader

Put the broker into maintenance mode first. This transfers all its partition leaderships away so clients see zero election blips (the chart's preStop hook also does this on graceful shutdown):

```bash
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk cluster maintenance enable 1

kubectl drain k3d-rp-cluster-agent-1 \
  --ignore-daemonsets --delete-emptydir-data
```

**What happens:**

- `redpanda-1` goes `Pending` — its PV has node affinity pinning it to the drained node (local-path provisioner). With network-attached storage the pod could follow the volume within the zone instead.
- The partition leader moves; produce/consume keep working on 2 of 3 brokers.
- `rpk cluster health` reports the topic as under-replicated — degraded, but available.

```bash
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic describe test -p
# PARTITION  LEADER  REPLICAS
# 0          2       [0 1 2]
```

### Scenario 2 — Bring the node back

```bash
kubectl uncordon k3d-rp-cluster-agent-1

kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk cluster maintenance disable 1
```

The pod remounts its old PVC, replays its durable Raft log, catches up on missed records, and rejoins. `rpk cluster health` returns to fully healthy.

### Scenario 3 — Try to drain a second node while one is down

```bash
kubectl drain k3d-rp-cluster-agent-2 --ignore-daemonsets
```

```
error when evicting pods/"redpanda-2" -n "redpanda":
Cannot evict pod as it would violate the pod's disruption budget.
```

**What happens:** Kubernetes blocks the eviction. With `maxUnavailable: 1` and one broker already down, the maintenance stalls instead of the cluster.

**Why it matters:** at 1 of 3 replicas, the survivor can't reach a Raft majority — partitions go **leaderless**, producers block, consumers stall. Redpanda never accepts quorum-less writes. Availability returns automatically when a second replica comes back, with no loss of acknowledged data.

> The PDB only guards *voluntary* disruptions. Two nodes dying simultaneously is what the zone spread is for.

### Scenario 4 — The node is never coming back

```bash
kubectl delete node k3d-rp-cluster-agent-2
```

The cluster still serves traffic on 2 of 3 brokers, but you're one failure from an outage and the PDB now blocks all maintenance.

**Key Redpanda difference:** brokers have cluster-assigned node IDs. A broker returning with the same pod name but an empty disk is a *new* broker, not a resurrected one. Decommission the dead one, then let a fresh one join:

```bash
# 1. Add a replacement node in the vacated zone
k3d node create rp-cluster-new-agent \
  --cluster rp-cluster \
  --k3s-node-label topology.kubernetes.io/zone=zone-c

# 2. Decommission the dead broker (ID 2)
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk redpanda admin brokers decommission 2

# Optional: watch replica movement (instant here, hours on real data volumes)
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk redpanda admin brokers decommission-status 2

# 3. Clean up orphaned storage and pod so the StatefulSet reschedules
kubectl -n redpanda delete pvc datadir-redpanda-2
kubectl -n redpanda delete pod redpanda-2
```

The recreated pod starts empty, joins via the seed brokers, and gets a **new node ID (3)** — pod ordinals and broker IDs no longer match, which is normal and harmless:

```bash
kubectl -n redpanda exec -it redpanda-0 -c redpanda -- rpk cluster health
# All nodes: [0 1 3]

kubectl -n redpanda exec -it redpanda-0 -c redpanda -- \
  rpk topic describe test -p
# PARTITION  LEADER  REPLICAS
# 0          0       [0 1 3]
```

All acknowledged data survives: everything written with `acks=all` was committed to a Raft majority, and a majority survived.

> 💡 The [Redpanda Operator](https://docs.redpanda.com/current/deploy/deployment-option/self-hosted/kubernetes/) automates this decommission-and-replace flow (ghost broker detection included). Recommended for production.

## Cleanup

```bash
k3d cluster delete rp-cluster
```

## Kafka vs Redpanda cheat sheet

| | Kafka | Redpanda |
|---|---|---|
| Replication | Leader/follower + ISR | Raft group per partition |
| Write durability | `acks=all` + `min.insync.replicas` | `acks=all` = Raft majority (fixed) |
| Coordinator | ZooKeeper / KRaft controllers | Built-in controller Raft group |
| Unclean leader election | Configurable (consistency trade-off) | Not possible |
| Planned drain | Manual leadership handling | `rpk cluster maintenance enable` |
| Dead broker recovery | Replacement reuses broker ID | Decommission → new broker, new node ID |

## Notes

- Command outputs are representative; exact `rpk` output and chart defaults vary by version.
- RF must be odd. RF=4 tolerates the same single failure as RF=3 — it just wastes a replica.

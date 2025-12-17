# NIKA Deployment Guide — v0.1

**Scope:** Internal / Trusted Environments  
**Status:** Production-ready (single-node)

---

## 1. Deployment Model

NIKA is designed to run as a **single-node, in-memory protocol server**.

### Intended usage
- Internal services
- Trusted clients
- Low-latency environments
- LAN / VPC / private networks

### Not intended for
- Public internet exposure
- Multi-tenant untrusted clients
- Compliance-critical workloads

---

## 2. Runtime Requirements

- Python 3.11
- ZeroMQ (pyzmq)
- Single process, single event loop
- No shared state across instances

---

## 3. Network Topology

NIKA uses **two sockets**:

| Plane | Socket Type | Purpose |
|-----|------------|--------|
| Control | ROUTER / DEALER | Auth, REQ/RES, lifecycle |
| Events | PUB / SUB | Events + Streams |

Example:
```
Control: tcp://127.0.0.1:5555
Events : tcp://127.0.0.1:5556
```

> Keep both sockets on a **private network only**.

---

## 4. Running the Server

```bash
python -m api.server
```

Recommended:
- Run under `systemd`, `supervisord`, or Docker
- One instance per node
- No horizontal scaling in v0.1

---

## 5. Configuration

All limits are currently **static and code-defined**:

- MAX_CLIENTS
- MAX_STREAMS_PER_CLIENT
- REQ_CACHE_TTL
- STREAM_ACL
- Stream rate limits

> External configuration is intentionally avoided in v0.1.

---

## 6. Scaling Guidance

### Supported
- Vertical scaling (CPU / RAM)
- More clients on same node

### Not Supported
- Multi-node clustering
- Load balancing
- State replication

> If you need scaling, run **multiple independent NIKA instances**.

---

## 7. Monitoring

Recommended metrics to observe:
- clients_active
- requests_total
- stream_sent
- stream_dropped

Use:
- Logs
- Prometheus sidecar (future)
- Internal dashboards

---

## 8. Restart & Failure Behavior

- All connections are dropped on restart
- No replay or recovery
- Clients must reconnect and re-subscribe

This is **expected behavior**.

---

## 9. Upgrade Strategy

- Protocol version enforced (`nika` field)
- Rolling upgrades require client compatibility
- No backward compatibility guarantee

---

## 10. Deployment Checklist

Before production:
- [ ] Network restricted
- [ ] API keys configured
- [ ] ACLs reviewed
- [ ] Limits tuned
- [ ] Logs enabled
- [ ] Clients tested for drops

---

**End of Deployment Guide**

"""Central definitions for loopback addresses and ports used across the charm.

Every MicroCloud service (Ceph mgr, MicroOVN, LXD) exposes its Prometheus
metrics endpoint on localhost.
"""

# Loopback address shared by all metrics exporters.
LOCALHOST_ADDR = "127.0.0.1"

# Default metrics ports for each service.
CEPH_METRICS_PORT = 9283
OVN_METRICS_PORT = 9310
LXD_METRICS_PORT = 8444

# Convenience "host:port" strings for the default ports.
CEPH_METRICS_ADDRESS = f"{LOCALHOST_ADDR}:{CEPH_METRICS_PORT}"
OVN_METRICS_ADDRESS = f"{LOCALHOST_ADDR}:{OVN_METRICS_PORT}"
LXD_METRICS_ADDRESS = f"{LOCALHOST_ADDR}:{LXD_METRICS_PORT}"

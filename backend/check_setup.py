from app.database import SessionLocal
from app.models.host import Host
from app.models.cluster import Cluster
from app.models.service import Service

db = SessionLocal()

print("=== HOSTS ===")
hosts = db.query(Host).all()
for h in hosts:
    print(f"  {h.id[:8]} | {h.hostname} | {h.ip_address} | status={h.status} | auth={h.auth_type}")

print("\n=== CLUSTERS ===")
clusters = db.query(Cluster).all()
for c in clusters:
    print(f"  {c.id[:8]} | {c.name} | kind={c.kind} | state={c.state} | version={c.kafka_version}")

print("\n=== SERVICES ===")
services = db.query(Service).all()
for s in services:
    print(f"  {s.id[:8]} | cluster={s.cluster_id[:8]} | role={s.role} | host={s.host_id[:8]} | node={s.node_id} | status={s.status}")

db.close()

from app.database import SessionLocal
from app.models.deployment_task import DeploymentTask

db = SessionLocal()
tasks = db.query(DeploymentTask).order_by(DeploymentTask.started_at.desc()).limit(5).all()
for t in tasks:
    err = (t.error_message or "none")[:100]
    print(f"{t.id[:8]} | {t.status} | {t.started_at} | {err}")
db.close()

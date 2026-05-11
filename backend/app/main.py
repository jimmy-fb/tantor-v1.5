import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import Base, engine, SessionLocal
from app.api import (
    hosts, clusters, ws, versions, topics, kafka_connect, security, ksqldb,
    auth, logs, monitoring, broker_config, rolling_restart,
    upgrades, security_scan, cluster_linking, rebalance, ldap, activity, alerts,
    schema_registry, external_clusters, security_tls, federation,
)
from app.models.kafka_user import KafkaUser  # noqa: F401 - ensure table creation
from app.models.audit_log import AuditLog  # noqa: F401 - ensure table creation
from app.models.query_history import QueryHistory  # noqa: F401 - ensure table creation
from app.models.user import User  # noqa: F401 - ensure table creation
from app.models.monitoring import MonitoringConfig  # noqa: F401 - ensure table creation
from app.models.config_audit import ConfigAuditLog  # noqa: F401 - ensure table creation

from app.models.cluster_link import ClusterLink  # noqa: F401 - ensure table creation
from app.models.ldap_config import LdapConfig  # noqa: F401 - ensure table creation
from app.models.deployment_task import DeploymentTask  # noqa: F401 - ensure table creation
from app.models.alert_rule import AlertRule  # noqa: F401 - ensure table creation
from app.models.notification_channel import NotificationChannel  # noqa: F401 - ensure table creation
from app.models.alert_incident import AlertIncident  # noqa: F401 - ensure table creation
from app.services.auth_service import AuthService
from app.services.migrations import apply_runtime_migrations

APP_VERSION = "1.4.3"

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOGGING_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tantor")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    # Add columns introduced after a table was first created in older installs.
    apply_runtime_migrations(engine)
    # Create default admin user on first run
    db = SessionLocal()
    try:
        AuthService.create_default_admin(db)
    finally:
        db.close()
    logger.info(f"Tantor {APP_VERSION} started")
    yield


app = FastAPI(
    title="Tantor",
    description="Kafka Cluster Deployment & Management",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    import traceback
    logger.error(f"Unhandled error: {exc}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


app.include_router(auth.router)
app.include_router(hosts.router)
app.include_router(clusters.router)
app.include_router(ws.router)
app.include_router(versions.router)
app.include_router(topics.router)
app.include_router(kafka_connect.router)
app.include_router(security.router)
app.include_router(ksqldb.router)
app.include_router(logs.router)
app.include_router(monitoring.router)
app.include_router(broker_config.router)
app.include_router(rolling_restart.router)

app.include_router(upgrades.router)
app.include_router(security_scan.router)
app.include_router(cluster_linking.router)
app.include_router(rebalance.router)
app.include_router(ldap.router)
app.include_router(activity.router)
app.include_router(alerts.cluster_router)
app.include_router(alerts.channel_router)
app.include_router(alerts.webhook_router)
app.include_router(schema_registry.router)
app.include_router(external_clusters.router)
app.include_router(security_tls.router)
app.include_router(federation.router)


@app.get("/api/health")
def health():
    return {"status": "ok", "version": APP_VERSION}

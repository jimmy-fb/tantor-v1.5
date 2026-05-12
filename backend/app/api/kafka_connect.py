from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.connect import ConnectorCreate
from app.services.connect_manager import connect_manager
from app.api.deps import require_admin, require_monitor_or_above
from app.models.user import User

router = APIRouter(prefix="/api/clusters/{cluster_id}/connect", tags=["kafka-connect"])


@router.get("/connectors")
def list_connectors(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return connect_manager.list_connectors(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.post("/connectors")
def create_connector(cluster_id: str, data: ConnectorCreate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        return connect_manager.create_connector(cluster_id, data.name, data.config, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.get("/connectors/{name}/status")
def get_connector_status(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return connect_manager.get_connector_status(cluster_id, name, db)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.get("/connectors/{name}/config")
def get_connector_config(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return connect_manager.get_connector_config(cluster_id, name, db)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.delete("/connectors/{name}")
def delete_connector(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        connect_manager.delete_connector(cluster_id, name, db)
        return {"detail": "Connector deleted"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.put("/connectors/{name}/pause")
def pause_connector(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        connect_manager.pause_connector(cluster_id, name, db)
        return {"detail": "Connector paused"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.put("/connectors/{name}/resume")
def resume_connector(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        connect_manager.resume_connector(cluster_id, name, db)
        return {"detail": "Connector resumed"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.post("/connectors/{name}/restart")
def restart_connector(cluster_id: str, name: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        connect_manager.restart_connector(cluster_id, name, db)
        return {"detail": "Connector restarted"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


@router.get("/plugins")
def list_plugins(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return connect_manager.get_plugins(cluster_id, db)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


# ── CDC quickstart templates ─────────────────────────────────────────────
# customer asked for "Stream real-time database changes directly into Kafka topics
# via CDC pipelines". Tantor ships pre-curated Debezium templates so the
# operator only fills in host / db / credentials, not the 30-key Connect
# config every time.
CDC_TEMPLATES: dict[str, dict] = {
    "debezium-mysql": {
        "id": "debezium-mysql",
        "name": "MySQL → Kafka (Debezium)",
        "connector_class": "io.debezium.connector.mysql.MySqlConnector",
        "description": "Stream row-level changes from a MySQL database via binlog into Kafka topics, one topic per table.",
        "fields": [
            {"key": "database.hostname", "label": "MySQL host", "required": True, "placeholder": "mysql.internal"},
            {"key": "database.port", "label": "MySQL port", "required": True, "default": "3306"},
            {"key": "database.user", "label": "Username (with REPLICATION CLIENT + REPLICATION SLAVE)", "required": True},
            {"key": "database.password", "label": "Password", "required": True, "secret": True},
            {"key": "database.server.id", "label": "Server ID (must be unique in the MySQL replication topology)", "required": True, "default": "184054"},
            {"key": "topic.prefix", "label": "Kafka topic prefix", "required": True, "placeholder": "mysql.acme"},
            {"key": "database.include.list", "label": "Databases to capture (comma-separated)", "required": True, "placeholder": "orders,customers"},
        ],
        "fixed": {
            "schema.history.internal.kafka.bootstrap.servers": "${BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-history",
            "include.schema.changes": "true",
        },
    },
    "debezium-postgres": {
        "id": "debezium-postgres",
        "name": "PostgreSQL → Kafka (Debezium)",
        "connector_class": "io.debezium.connector.postgresql.PostgresConnector",
        "description": "Stream row-level changes from a PostgreSQL database via logical replication into Kafka topics.",
        "fields": [
            {"key": "database.hostname", "label": "Postgres host", "required": True, "placeholder": "pg.internal"},
            {"key": "database.port", "label": "Postgres port", "required": True, "default": "5432"},
            {"key": "database.user", "label": "Username (with REPLICATION privilege)", "required": True},
            {"key": "database.password", "label": "Password", "required": True, "secret": True},
            {"key": "database.dbname", "label": "Database name", "required": True},
            {"key": "topic.prefix", "label": "Kafka topic prefix", "required": True, "placeholder": "pg.acme"},
            {"key": "table.include.list", "label": "Tables to capture (comma-separated, schema.table)", "required": False, "placeholder": "public.orders,public.customers"},
            {"key": "plugin.name", "label": "Logical decoding plugin", "required": True, "default": "pgoutput"},
        ],
        "fixed": {
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": "initial",
        },
    },
    "debezium-mongodb": {
        "id": "debezium-mongodb",
        "name": "MongoDB → Kafka (Debezium)",
        "connector_class": "io.debezium.connector.mongodb.MongoDbConnector",
        "description": "Stream change events from a MongoDB replica set or sharded cluster via the change stream API.",
        "fields": [
            {"key": "mongodb.connection.string", "label": "MongoDB connection string", "required": True, "placeholder": "mongodb://user:pass@host1:27017,host2:27017/?replicaSet=rs0"},
            {"key": "topic.prefix", "label": "Kafka topic prefix", "required": True, "placeholder": "mongo.acme"},
            {"key": "database.include.list", "label": "Databases to capture", "required": False},
            {"key": "collection.include.list", "label": "Collections to capture", "required": False},
        ],
        "fixed": {
            "snapshot.mode": "initial",
        },
    },
    "debezium-sqlserver": {
        "id": "debezium-sqlserver",
        "name": "SQL Server → Kafka (Debezium)",
        "connector_class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "description": "Stream changes from a SQL Server database via SQL Server's CDC feature into Kafka topics.",
        "fields": [
            {"key": "database.hostname", "label": "SQL Server host", "required": True},
            {"key": "database.port", "label": "Port", "required": True, "default": "1433"},
            {"key": "database.user", "label": "Username", "required": True},
            {"key": "database.password", "label": "Password", "required": True, "secret": True},
            {"key": "database.names", "label": "Databases to capture (comma-separated)", "required": True},
            {"key": "topic.prefix", "label": "Kafka topic prefix", "required": True, "placeholder": "mssql.acme"},
        ],
        "fixed": {
            "schema.history.internal.kafka.bootstrap.servers": "${BOOTSTRAP}",
            "schema.history.internal.kafka.topic": "schema-history",
        },
    },
}


@router.get("/cdc/templates")
def list_cdc_templates(
    cluster_id: str,
    _: User = Depends(require_monitor_or_above),
):
    """Return the pre-curated CDC connector templates the UI wizard renders.

    Each template lists user-fillable fields plus internal `fixed` keys that
    Tantor injects (with ${BOOTSTRAP} substituted from the cluster's actual
    bootstrap servers when create_cdc_connector is called).
    """
    return list(CDC_TEMPLATES.values())


class CdcCreateRequest(BaseModel):
    name: str
    template_id: str
    fields: dict


@router.post("/cdc/create")
def create_cdc_connector(
    cluster_id: str,
    data: CdcCreateRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Materialize a CDC template into a real Connect connector.

    Tantor merges the user-supplied fields with the template's `fixed` keys,
    substitutes ${BOOTSTRAP} from the cluster's bootstrap servers, and POSTs
    the resulting config to the Connect REST API. Returns the standard
    Connect status response.
    """
    tpl = CDC_TEMPLATES.get(data.template_id)
    if not tpl:
        raise HTTPException(status_code=404, detail=f"Unknown CDC template: {data.template_id}")
    config = {"connector.class": tpl["connector_class"]}
    config.update(data.fields)

    # Look up bootstrap so ${BOOTSTRAP} placeholders resolve. For managed
    # clusters this is constructed from the broker host + listener port; for
    # external it lives on the cluster row.
    from app.models.cluster import Cluster
    cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    bootstrap = cluster.bootstrap_servers or _derive_bootstrap(cluster, db)
    for k, v in tpl.get("fixed", {}).items():
        config[k] = (v or "").replace("${BOOTSTRAP}", bootstrap or "")

    try:
        return connect_manager.create_connector(cluster_id, data.name, config, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Connect API error: {e}")


def _derive_bootstrap(cluster, db: Session) -> str:
    """Build a bootstrap.servers string from the broker hosts of a managed cluster."""
    from app.models.service import Service
    from app.models.host import Host
    services = db.query(Service).filter(Service.cluster_id == cluster.id).all()
    parts: list[str] = []
    cfg = {}
    try:
        import json as _json
        cfg = _json.loads(cluster.config_json or "{}")
    except Exception:
        cfg = {}
    port = cfg.get("listener_port", 9092)
    for svc in services:
        if "broker" in (svc.role or ""):
            host = db.query(Host).filter(Host.id == svc.host_id).first()
            if host:
                parts.append(f"{host.ip_address}:{port}")
    return ",".join(parts)

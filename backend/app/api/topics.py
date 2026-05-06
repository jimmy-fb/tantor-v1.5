from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.topic import (
    TopicCreate, ProduceRequest, ProduceResponse,
    ConsumeRequest, ConsumeResponse, ConsumedMessage,
    ValidationResult, ValidationStep,
)
from app.services.kafka_admin import kafka_admin
from app.api.deps import require_admin, require_monitor_or_above
from app.models.user import User

router = APIRouter(prefix="/api/clusters/{cluster_id}", tags=["kafka-admin"])


@router.get("/topics")
def list_topics(cluster_id: str, search: str | None = None, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        topics = kafka_admin.list_topics(cluster_id, db)
        if search:
            topics = [t for t in topics if search.lower() in t["name"].lower()]
        return topics
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/topics")
def create_topic(cluster_id: str, data: TopicCreate, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        return kafka_admin.create_topic(
            cluster_id, data.name, data.partitions, data.replication_factor, data.config or {}, db
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/topics/{topic_name}")
def get_topic(cluster_id: str, topic_name: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return kafka_admin.get_topic_detail(cluster_id, topic_name, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/topics/{topic_name}")
def delete_topic(cluster_id: str, topic_name: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        return kafka_admin.delete_topic(cluster_id, topic_name, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/topics/{topic_name}/config")
def update_topic_config(cluster_id: str, topic_name: str, body: dict, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    """Update topic configuration (e.g. retention.ms, cleanup.policy)."""
    try:
        return kafka_admin.alter_topic_config(cluster_id, topic_name, body["configs"], db, actor=current_user)
    except KeyError:
        raise HTTPException(status_code=422, detail="Request body must include 'configs' dict")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/topics/{topic_name}/partitions")
def update_topic_partitions(cluster_id: str, topic_name: str, body: dict, db: Session = Depends(get_db), current_user: User = Depends(require_admin)):
    """Increase the number of partitions for a topic."""
    try:
        return kafka_admin.increase_partitions(cluster_id, topic_name, body["count"], db, actor=current_user)
    except KeyError:
        raise HTTPException(status_code=422, detail="Request body must include 'count' integer")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/consumer-groups")
def list_consumer_groups(cluster_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return kafka_admin.list_consumer_groups(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/consumer-groups/{group_id}")
def get_consumer_group(cluster_id: str, group_id: str, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    try:
        return kafka_admin.get_consumer_group_detail(cluster_id, group_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/produce", response_model=ProduceResponse)
def produce_message(cluster_id: str, data: ProduceRequest, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    try:
        return kafka_admin.produce_message(cluster_id, data.topic, data.key, data.value, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/consume", response_model=ConsumeResponse)
def consume_messages(cluster_id: str, data: ConsumeRequest, db: Session = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    """Consume messages from a topic with full metadata (partition, offset, timestamp)."""
    try:
        messages = kafka_admin.consume_messages(
            cluster_id, data.topic, db,
            from_beginning=data.from_beginning,
            max_messages=data.max_messages,
            group_id=data.group_id,
            timeout_ms=data.timeout_ms,
        )
        return ConsumeResponse(
            messages=[ConsumedMessage(**m) for m in messages],
            count=len(messages),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Consume failed: {e}")


@router.post("/validate", response_model=ValidationResult)
def validate_cluster(
    cluster_id: str,
    create_test_topic: bool = True,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Run post-install Kafka validation: list topics, create test, produce/consume."""
    try:
        result = kafka_admin.validate_cluster(cluster_id, db, create_test_topic=create_test_topic)
        return ValidationResult(
            steps=[ValidationStep(**s) for s in result["steps"]],
            success=result["success"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

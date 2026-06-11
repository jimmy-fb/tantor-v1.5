from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.kafka_admin import kafka_admin
from app.api.deps import require_admin
from app.models.user import User

router = APIRouter(prefix="/api/clusters/{cluster_id}", tags=["rebalance"])


class GeneratePlanRequest(BaseModel):
    topics: list[str]
    broker_ids: list[int]


class ExecuteRequest(BaseModel):
    reassignment: dict


class VerifyRequest(BaseModel):
    reassignment: dict


@router.get("/partitions/distribution")
def get_partition_distribution(
    cluster_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Get partition distribution across brokers."""
    try:
        return kafka_admin.get_partition_distribution(cluster_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/partitions/generate-plan")
def generate_reassignment_plan(
    cluster_id: str,
    data: GeneratePlanRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Generate a partition reassignment plan."""
    try:
        return kafka_admin.generate_reassignment_plan(
            cluster_id, data.topics, data.broker_ids, db
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/partitions/execute")
def execute_reassignment(
    cluster_id: str,
    data: ExecuteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Execute a partition reassignment plan."""
    try:
        return kafka_admin.execute_reassignment(cluster_id, data.reassignment, db, actor=current_user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/partitions/verify")
def verify_reassignment(
    cluster_id: str,
    data: VerifyRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Verify the status of a partition reassignment."""
    try:
        return kafka_admin.verify_reassignment(cluster_id, data.reassignment, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

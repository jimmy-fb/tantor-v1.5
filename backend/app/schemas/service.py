from pydantic import BaseModel


class ServiceStatusUpdate(BaseModel):
    status: str


class ServiceActionResponse(BaseModel):
    service_id: str
    action: str
    success: bool
    message: str
    # v1.5 — which transport actually ran the action: "agent" | "ssh".
    # Optional so old code paths that don't set it (kept on the SSH-only
    # branches we haven't fully retrofitted yet) still serialize cleanly.
    via: str | None = None

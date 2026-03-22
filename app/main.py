from __future__ import annotations

import os

from fastapi import FastAPI

from app.routers.admin import router as admin_router
from app.routers.auth import router as auth_router
from app.routers.device_layout import router as device_layout_router
from app.routers.legacy_data import router as legacy_data_router
from app.routers.message_board import router as message_board_router
from app.routers.member import router as member_router
from app.routers.points_flow import router as points_flow_router
from app.routers.reconciliation import router as reconciliation_router
from app.routers.tools import router as tools_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="FastAPI Points Flow",
        version="0.1.0",
        description="FastAPI backend for ICSP points flow authentication and export tasks.",
    )

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(auth_router)
    app.include_router(admin_router)
    app.include_router(device_layout_router)
    app.include_router(legacy_data_router)
    app.include_router(message_board_router)
    app.include_router(member_router)
    app.include_router(points_flow_router)
    app.include_router(reconciliation_router)
    app.include_router(tools_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "8000")),
        reload=False,
        workers=1,
    )

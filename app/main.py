from __future__ import annotations

import os

from fastapi import FastAPI

from app.routers.points_flow import router as points_flow_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="FastAPI Points Flow",
        version="0.1.0",
        description="Minimal FastAPI backend for simulated points flow export tasks.",
    )

    @app.get("/health", tags=["system"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(points_flow_router)
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

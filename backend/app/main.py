from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.api.routes import router as api_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="ACE-SEM API",
        version="1.0.0",
        docs_url="/sem-api/docs",
        redoc_url="/sem-api/redoc",
        openapi_url="/sem-api/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/sem-api")
    
    return app


app = create_app()






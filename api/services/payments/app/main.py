import asyncio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv
from cache.redis_cache import RedisCache

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from database.connection import db_connection
from routes.payments_routes import router as payment_router
from services.payments_grpc_server import serve_grpc

load_dotenv()

HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', '8001'))
GRPC_PORT = int(os.getenv('GRPC_PORT', '50051'))
RELOAD = os.getenv('RELOAD', 'True').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'info')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
CORS_ORIGINS = os.getenv('CORS_ORIGINS', 'http://localhost:3000,http://127.0.0.1:3000')
LOG_FILE = os.getenv('LOG_FILE', 'payments_service.log')

ALLOWED_ORIGINS = [origin.strip() for origin in CORS_ORIGINS.split(',')]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE)
    ]
)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Payments Service...")
    logger.info(f"Environment: {ENVIRONMENT}")
    logger.info(f"HTTP Host: {HOST}, HTTP Port: {PORT}")
    logger.info(f"gRPC Port: {GRPC_PORT}")
    logger.info(f"Stripe Mode: {os.getenv('STRIPE_MODE', 'test')}")
    
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint="http://otel-collector:4318/v1/traces"),
        )
    )
    trace.set_tracer_provider(tracer_provider)
    
    await db_connection.connect()
    
    redis_cache = RedisCache()
    try:
        await redis_cache.connect()
        logger.info("Redis connected for idempotency")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}. Idempotency disabled.")
        redis_cache = None
    
    grpc_task = asyncio.create_task(
        serve_grpc(redis_cache=redis_cache, port=GRPC_PORT)
    )
    
    app.state.grpc_task = grpc_task
    app.state.redis_cache = redis_cache
    
    yield
    
    logger.info("Shutting down Payments Service...")
    
    if hasattr(app.state, 'grpc_task'):
        app.state.grpc_task.cancel()
        try:
            await app.state.grpc_task
        except asyncio.CancelledError:
            pass

    if hasattr(app.state, 'redis_cache'):
        await app.state.redis_cache.close()

app = FastAPI(
    title="Ecommerce API - Payments Service",
    description="Payment processing microservice with Stripe integration",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

FastAPIInstrumentor.instrument_app(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception handler: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "type": "https://example.com/errors/internal",
            "title": "Internal Server Error",
            "status": 500,
            "detail": "An unexpected error occurred",
            "instance": str(request.url)
        },
        media_type="application/problem+json"
    )

@app.get("/routes", tags=["Debug"])
async def list_all_routes(request: Request):
    routes = []
    for route in request.app.routes:
        route_info = {
            "path": route.path,
            "methods": list(route.methods) if hasattr(route, 'methods') else [],
            "name": route.name if hasattr(route, 'name') else None,
            "endpoint": str(route.endpoint) if hasattr(route, 'endpoint') else None
        }
        routes.append(route_info)
    
    return {
        "service": "payments",
        "total_routes": len(routes),
        "routes": routes
    }
    
@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "service": "payments",
        "timestamp": "2024-01-01T00:00:00Z",
        "environment": ENVIRONMENT,
        "stripe_mode": os.getenv('STRIPE_MODE', 'test'),
        "grpc_port": GRPC_PORT
    }

@app.get("/ready", tags=["Health"])
async def readiness_check():
    return {
        "status": "ready",
        "service": "payments",
        "timestamp": "2024-01-01T00:00:00Z",
        "grpc_ready": True
    }

@app.get("/info", tags=["Root"])
async def root():
    return {
        "message": "Ecommerce Payments Service",
        "version": "1.0.0",
        "environment": ENVIRONMENT,
        "stripe_mode": os.getenv('STRIPE_MODE', 'test'),
        "ports": {
            "http": PORT,
            "grpc": GRPC_PORT
        },
        "docs": "/docs",
        "health": "/health",
        "ready": "/ready"
    }

app.include_router(payment_router)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=HOST,
        port=PORT,
        reload=RELOAD,
        log_level=LOG_LEVEL
    )
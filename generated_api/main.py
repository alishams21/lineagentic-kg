#!/usr/bin/env python3
"""
Main FastAPI application
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import models
import get_routes
import upsert_routes
import delete_routes


def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    app = FastAPI(
        title="RegistryFactory Generated API",
        description="Auto-generated API from RegistryFactory methods",
        version="1.0.0"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers
    app.include_router(get_routes.router)
    app.include_router(upsert_routes.router)
    app.include_router(delete_routes.router)
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    print(f"🚀 Starting RegistryFactory API server on {host}:{port}")
    print(f" API Documentation: http://{host}:{port}/docs")
    
    uvicorn.run(app, host=host, port=port)

"""ASGI entrypoint. `uvicorn main:app` keeps serving the original root route."""
from fleetapi.app import app  # noqa: F401

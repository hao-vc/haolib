"""Test idempotency."""

import pytest
from fastapi import FastAPI, status
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_idempotency(clean_redis: None, app: FastAPI, test_client: AsyncClient) -> None:
    """Test idempotency."""

    @app.post("/api/orders")
    async def create_order() -> str:
        """Create order."""
        return "Something"

    response = await test_client.post(
        "/api/orders", json={"product_id": 123, "quantity": 1}, headers={"Idempotency-Key": "123"}
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == "Something"

    response = await test_client.post(
        "/api/orders", json={"product_id": 123, "quantity": 1}, headers={"Idempotency-Key": "123"}
    )

    assert response.status_code == status.HTTP_409_CONFLICT
    assert response.json() == {
        "detail": "Idempotent request",
        "error_code": "IDEMPOTENT_REQUEST",
        "additional_info": {},
    }

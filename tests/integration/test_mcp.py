"""Test MCP."""

import json
from typing import TYPE_CHECKING

import pytest
from fastmcp.client import Client
from mcp.types import TextContent

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastmcp import FastMCP


@pytest.mark.asyncio
async def test_mcp(app_with_mcp_and_mcp: tuple[FastAPI, FastMCP]) -> None:
    """Test MCP."""

    async with Client(app_with_mcp_and_mcp[1]) as client:
        result = await client.call_tool(
            "hello_hello_post",
            {
                "name": "Wireless Keyboard",
                "price": 79.99,
                "category": "Electronics",
                "description": "Bluetooth mechanical keyboard",
            },
        )

        assert isinstance(result.content[0], TextContent)

        assert json.loads(result.content[0].text)["result"] == "hello"

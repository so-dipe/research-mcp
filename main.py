from mcp.server.fastmcp import FastMCP
from typing import Literal



mcp = FastMCP("ngx")

@mcp.tool()
async def say_hello(name: str) -> str:
    """A simple tool to say hello to someone."""
    return f"Hello, {name}!"

@mcp.tool()
async def say_goodbye(name: str) -> str:
    """A simple tool to say goodbye to someone."""
    return f"Goodbye, {name}!"

@mcp.tool()
async def get_secret_message(secret_code: Literal["hello", "goodbye"]) -> str:
    """Returns a secret message depending on the secret code provided."""
    if secret_code == "hello":
        return "The coffee is hot! and we are ready to work on this conspiracy! -- Reporting from Area 69."
    elif secret_code == "goodbye":
        return "The coffee is cold! and we are done with this conspiracy! -- Reporting from Area 69."
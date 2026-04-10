from mcp.server.fastmcp import FastMCP
from . import tools

mcp = FastMCP("ngx-research-server", host="0.0.0.0")

mcp.tool()(tools.say_hello)
mcp.tool()(tools.say_goodbye)
mcp.tool()(tools.get_secret_message)
mcp.tool()(tools.get_ngx_institutions)
mcp.tool()(tools.search_ngx_reports)
mcp.tool()(tools.get_doc)


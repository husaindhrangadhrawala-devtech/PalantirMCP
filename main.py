from mcp.server.fastmcp.server import FastMCP, Image
from typing_extensions import TypedDict
from typing import Annotated, List, Literal, Optional
from operator import add
global mcp

mcp = FastMCP("MyApp")

class Context(TypedDict):
    user: str
    question: str
    chat_history: Annotated[List[dict], add]
    steps: Annotated[List[str], add]
    next_action: str
    messages: Annotated[List[dict], add]
    
if __name__ == "__main__":
    print("Starting server...")
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
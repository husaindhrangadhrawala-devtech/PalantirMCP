from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from typing_extensions import TypedDict
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import SystemMessage, RemoveMessage
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.checkpoint.memory import MemorySaver
from typing import Annotated, List, Literal, Optional
from langgraph.errors import NodeInterrupt, Interrupt
from fastmcp.client.transports import stdio_client
from mcp import StdioServerParameters, ClientSession
from contextlib import AsyncExitStack
from anthropic import Anthropic
import asyncio
import json

class MCPClient:
    def __init__(self):
        self.session:Optional[ClientSession]=None
        self.exit_stack = AsyncExitStack()
        self.anthropic = Anthropic()

    async def connect(self,server_script_path):
        try:
            
            server_params = StdioServerParameters(command="python",args=[server_script_path],env=None)
            
            stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
            
            self.stdio, self.write = stdio_transport
            
            self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))
            
            await self.session.initialize()
            
            # resource_list = await self.session.list_resources()
            # tools = await self.session.list_tools()

            # resource=await self.session.read_resource((resource_list.resources)[0].uri)
            # metadata = json.loads((resource.contents)[0].text)
            # return metadata
        except GeneratorExit:
            await self.exit_stack.aclose()
        except Exception as e:
            print(f"Error during connection: {e}")
            await self.cleanup()
            raise

    async def list_resources(self):
        return await self.session.list_resources()
    async def list_tools(self):
        return await self.session.list_tools()
    async def cleanup(self):
        """Properly cleanup resources"""
        if self.exit_stack:
            try:
                await self.exit_stack.aclose()
            except Exception as e:
                print(f"Error during cleanup: {e}")
            finally:
                self.exit_stack = None
                self.session = None
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()




async def main():
    path = f""  # Path to MCP Server script
    async with MCPClient() as client:
        await client.connect(path)
        resources = await client.list_resources()
        tools = await client.list_tools()
        print("Resources:", resources)  
        print("Tools:", tools)
        # print("Metadata:", metadata)
    
    # resources = await client.list_resources()
    # tools = await client.list_tools()
    # print("Resources:", resources)
    # print("Tools:", tools)

if __name__ == "__main__":
    asyncio.run(main())


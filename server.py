import os, json, hashlib
from typing import Optional
from datetime import datetime, date
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine, inspect, text
import logging
import asyncio

# Load environment variables
load_dotenv()

# Logging konfigurieren
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mcp_alchemy")

### Database ###

def get_engine():
    connection_string = (
        "mssql+pymssql://"
        "heidi_user:Arschloch1985!@127.0.0.1:1433/"
        "Ikarus_LVDB_Test"
        "?charset=utf8&appname=MSOLEDBSQL"
    )
    return create_engine(connection_string)

def get_db_info():
    engine = get_engine()
    with engine.connect() as conn:
        url = engine.url
        return (f"Connected to {engine.dialect.name} "
                f"database '{url.database}' on {url.host} "
                f"as user '{url.username}'")

### Constants ###
DB_INFO = get_db_info()
EXECUTE_QUERY_MAX_CHARS = int(os.environ.get('EXECUTE_QUERY_MAX_CHARS', 4000))
CLAUDE_FILES_PATH = os.environ.get('CLAUDE_LOCAL_FILES_PATH')

### MCP ###
def create_mcp():
    mcp = FastMCP("MCP Alchemy")
    
    @mcp.tool("List all tables in the database")
    async def all_table_names() -> str:
        try:
            engine = get_engine()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Found {len(tables)} tables")
            return ", ".join(tables)
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return f"Error: {str(e)}"
    
    @mcp.tool("Filter tables by name")
    async def filter_table_names(q: str) -> str:
        try:
            engine = get_engine()
            inspector = inspect(engine)
            tables = [x for x in inspector.get_table_names() if q in x]
            logger.info(f"Found {len(tables)} tables matching '{q}'")
            return ", ".join(tables)
        except Exception as e:
            logger.error(f"Error filtering tables: {e}")
            return f"Error: {str(e)}"

    @mcp.tool("Get schema information")
    async def schema_definitions(table_names: list[str]) -> str:
        try:
            engine = get_engine()
            inspector = inspect(engine)
            result = []
            for table_name in table_names:
                columns = inspector.get_columns(table_name)
                result.append(f"{table_name}:")
                for col in columns:
                    result.append(f"  {col['name']}: {col['type']}")
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Error getting schema: {e}")
            return f"Error: {str(e)}"

    def execute_query_description():
        parts = [
            f"Execute a SQL query and return results in a readable format. Results will be truncated after {EXECUTE_QUERY_MAX_CHARS} characters."
        ]
        if CLAUDE_FILES_PATH:
            parts.append("Claude Desktop may fetch the full result set via an url for analysis and artifacts.")
        parts.append(DB_INFO)
        return " ".join(parts)

    @mcp.tool("Execute SQL query")
    async def execute_query(query: str) -> str:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query))
                if not result.returns_rows:
                    return f"Success: {result.rowcount} rows affected"
                return "\n".join(str(row) for row in result)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return f"Error: {str(e)}"

    return mcp

if __name__ == "__main__":
    try:
        logger.info("Starting MCP server...")
        mcp = create_mcp()
        mcp.run()
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise

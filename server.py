from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine, inspect, text
from typing import Optional
from datetime import datetime, date
import os

def get_engine():
    return create_engine("mssql+pymssql://heidi_user:Arschloch1985!@127.0.0.1:1433/Ikarus_LVDB_Test")

mcp = FastMCP("MCP Alchemy")

@mcp.tool("all_table_names")
async def all_table_names() -> str:
    """Return all table names in the database separated by comma."""
    engine = get_engine()
    inspector = inspect(engine)
    return ", ".join(inspector.get_table_names())

@mcp.tool("filter_table_names")
async def filter_table_names(q: str) -> str:
    """Find tables matching a substring."""
    engine = get_engine()
    inspector = inspect(engine)
    return ", ".join(x for x in inspector.get_table_names() if q in x)

@mcp.tool("schema_definitions")
async def schema_definitions(table_names: list[str]) -> str:
    """Get detailed schema for specified tables."""
    engine = get_engine()
    inspector = inspect(engine)
    
    def format_table(table_name: str) -> str:
        columns = inspector.get_columns(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        primary_keys = set(inspector.get_pk_constraint(table_name)["constrained_columns"])
        
        result = [f"{table_name}:"]
        
        # Columns
        for column in columns:
            attrs = []
            if column["name"] in primary_keys:
                attrs.append("primary key")
            if column.get("autoincrement"):
                attrs.append("autoincrement")
            if column["nullable"]:
                attrs.append("nullable")
            
            result.append(f"    {column['name']}: {column['type']}" + 
                         (f", {', '.join(attrs)}" if attrs else ""))
        
        # Relationships
        if foreign_keys:
            result.extend(["", "    Relationships:"])
            for fk in foreign_keys:
                result.append(f"      {', '.join(fk['constrained_columns'])} -> "
                            f"{fk['referred_table']}.{', '.join(fk['referred_columns'])}")
        
        return "\n".join(result)
    
    return "\n\n".join(format_table(table) for table in table_names)

# ... vorheriger Code bleibt gleich ...

@mcp.tool("execute_query")
async def execute_query(query: str | dict, params: Optional[dict] = None) -> str:
    """Execute SQL query with vertical output format."""
    def format_value(val):
        if val is None:
            return "NULL"
        if isinstance(val, (datetime, date)):
            return val.isoformat()
        return str(val)

    try:
        # Handle dict input
        if isinstance(query, dict):
            query = query.get('query', '')
        
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            
            if not result.returns_rows:
                return f"Success: {result.rowcount} rows affected"
            
            columns = result.keys()
            rows = result.fetchall()
            
            if not rows:
                return "No rows returned"
            
            # Format in vertical style
            output = []
            for i, row in enumerate(rows, 1):
                output.append(f"{i}. row")
                for col, val in zip(columns, row):
                    output.append(f"{col}: {format_value(val)}")
                output.append("")
            
            output.append(f"Result: {len(rows)} rows")
            return "\n".join(output)
            
    except Exception as e:
        return f"Error: {str(e)}"

# ... Rest des Codes bleibt gleich ...

if __name__ == "__main__":
    mcp.run()
"""PostgreSQL database client for HyeAero ETL pipeline.

Handles connection management, schema creation, and basic database operations.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import sql
from typing import Optional, Dict, List, Any
from pathlib import Path
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class PostgresClient:
    """PostgreSQL database client for ETL pipeline operations."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
    ):
        """Initialize PostgreSQL client.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            connection_string: Full connection URI (overrides individual params)
        """
        if connection_string:
            # Parse connection string if provided
            self.connection_string = connection_string
        else:
            # Build from individual parameters or environment variables
            self.host = host or os.getenv("POSTGRES_HOST", "localhost")
            self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
            self.database = database or os.getenv("POSTGRES_DATABASE", "defaultdb")
            self.user = user or os.getenv("POSTGRES_USER", "postgres")
            self.password = password or os.getenv("POSTGRES_PASSWORD", "")
            
            self.connection_string = (
                f"host={self.host} port={self.port} dbname={self.database} "
                f"user={self.user} password={self.password} sslmode=require"
            )

    @contextmanager
    def get_connection(self):
        """Get database connection context manager."""
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as list of dicts.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result rows as dictionaries
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.rowcount

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            params_list: List of parameter tuples

        Returns:
            Total number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, params_list)
                return cur.rowcount

    def create_schema(self, schema_file: Optional[Path] = None) -> bool:
        """Create database schema from SQL file.

        Args:
            schema_file: Path to schema SQL file. If None, uses default schema.sql

        Returns:
            True if successful
        """
        if schema_file is None:
            schema_file = Path(__file__).parent / "schema.sql"

        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False

        logger.info(f"Creating database schema from {schema_file}")
        sql_content = schema_file.read_text(encoding="utf-8")

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_content)
            logger.info("Database schema created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema: {e}", exc_info=True)
            raise

    def test_connection(self) -> bool:
        """Test database connection.

        Returns:
            True if connection successful
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    if result:
                        logger.info("Database connection test successful")
                        return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
        return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0]["exists"] if result else False

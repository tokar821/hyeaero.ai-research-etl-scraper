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
                # Execute statements individually to avoid transaction rollback issues
                # This way, if one statement fails, others that succeeded are already committed
                statements = self._split_sql_statements(sql_content)
                logger.info(f"Executing {len(statements)} schema statements individually...")
                
                executed = 0
                skipped = 0
                errors = []
                
                with conn.cursor() as cur:
                    for i, statement in enumerate(statements, 1):
                        if not statement.strip():
                            continue
                            
                        try:
                            cur.execute(statement)
                            conn.commit()  # Commit after each successful statement
                            executed += 1
                        except Exception as stmt_error:
                            error_msg = str(stmt_error)
                            # Ignore "already exists" errors - these are expected and harmless
                            if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
                                conn.commit()  # Commit anyway (object already exists is fine)
                                skipped += 1
                                if skipped <= 3:  # Log first few skips
                                    logger.debug(f"Statement {i} skipped (already exists): {error_msg[:100]}")
                            else:
                                # Log other errors but continue
                                errors.append(f"Statement {i}: {error_msg[:200]}")
                                logger.warning(f"Statement {i} failed (continuing): {error_msg[:200]}")
                                conn.rollback()  # Rollback this statement only
                    
                    if errors:
                        logger.warning(f"Schema creation completed with {len(errors)} errors (executed {executed}, skipped {skipped})")
                    else:
                        logger.info(f"Database schema created/verified successfully (executed {executed}, skipped {skipped})")
                    
                    # Verify critical tables exist
                    critical_tables = ['aircraft', 'aircraft_listings', 'faa_registrations']
                    missing_tables = []
                    for table in critical_tables:
                        check_table = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table}')"
                        cur.execute(check_table)
                        exists = cur.fetchone()[0]
                        if not exists:
                            missing_tables.append(table)
                    
                    if missing_tables:
                        logger.error(f"Critical tables missing after schema creation: {missing_tables}")
                        logger.error("Schema creation may have failed. Please check database connection and permissions.")
                        raise Exception(f"Schema creation incomplete: missing tables {missing_tables}")
                    
                    logger.info(f"Verified critical tables exist: {', '.join(critical_tables)}")
                    
                    # If aircraft table exists, check if we need to fix serial_number constraint
                    check_query = """
                        SELECT column_name, is_nullable 
                        FROM information_schema.columns 
                        WHERE table_name = 'aircraft' AND column_name = 'serial_number'
                    """
                    cur.execute(check_query)
                    result = cur.fetchone()
                    if result and result[1] == 'NO':
                        # serial_number is NOT NULL, need to fix it
                        logger.info("Fixing serial_number constraint to allow NULL...")
                        fix_sql = """
                            ALTER TABLE aircraft ALTER COLUMN serial_number DROP NOT NULL;
                            ALTER TABLE aircraft DROP CONSTRAINT IF EXISTS aircraft_serial_number_key;
                            ALTER TABLE aircraft DROP CONSTRAINT IF EXISTS at_least_one_identifier;
                            ALTER TABLE aircraft ADD CONSTRAINT at_least_one_identifier 
                                CHECK (serial_number IS NOT NULL OR registration_number IS NOT NULL);
                            CREATE UNIQUE INDEX IF NOT EXISTS idx_aircraft_serial_number_unique 
                                ON aircraft(serial_number) WHERE serial_number IS NOT NULL;
                        """
                        cur.execute(fix_sql)
                        logger.info("serial_number constraint fixed")
                        
            logger.info("Database schema created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema: {e}", exc_info=True)
            raise
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """Split SQL content into individual statements.
        
        Handles:
        - Multi-line statements
        - Comments (-- and /* */)
        - Strings (single and double quotes)
        - Dollar-quoted strings (for function bodies)
        
        Args:
            sql_content: Full SQL file content
            
        Returns:
            List of individual SQL statements
        """
        statements = []
        current_statement = []
        in_block_comment = False
        in_string = False
        string_char = None
        in_dollar_quote = False
        dollar_tag = None
        
        i = 0
        content = sql_content
        
        while i < len(content):
            char = content[i]
            next_char = content[i+1] if i+1 < len(content) else None
            prev_char = content[i-1] if i > 0 else None
            
            # Handle block comments
            if not in_string and not in_dollar_quote and char == '/' and next_char == '*':
                in_block_comment = True
                i += 2
                continue
            if in_block_comment and char == '*' and next_char == '/':
                in_block_comment = False
                i += 2
                continue
            if in_block_comment:
                i += 1
                continue
            
            # Handle line comments (only at start of line or after whitespace)
            if not in_string and not in_dollar_quote and char == '-' and next_char == '-':
                # Skip rest of line
                while i < len(content) and content[i] != '\n':
                    i += 1
                if i < len(content):
                    current_statement.append('\n')
                    i += 1
                continue
            
            # Handle dollar-quoted strings ($$ or $tag$)
            if not in_string and not in_block_comment:
                if char == '$':
                    # Check for dollar quote start
                    j = i + 1
                    tag = ''
                    while j < len(content) and content[j] != '$':
                        tag += content[j]
                        j += 1
                    if j < len(content):  # Found closing $
                        if not in_dollar_quote:
                            # Starting dollar quote
                            in_dollar_quote = True
                            dollar_tag = tag
                            current_statement.append('$' + tag + '$')
                            i = j + 1
                            continue
                        elif tag == dollar_tag:
                            # Ending dollar quote
                            in_dollar_quote = False
                            dollar_tag = None
                            current_statement.append('$' + tag + '$')
                            i = j + 1
                            continue
                
                if in_dollar_quote:
                    current_statement.append(char)
                    i += 1
                    continue
            
            # Handle regular strings
            if not in_dollar_quote and (char == "'" or char == '"'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    # Check if escaped (not \' or \")
                    if prev_char != '\\' or (i > 1 and content[i-2] == '\\'):
                        in_string = False
                        string_char = None
                current_statement.append(char)
                i += 1
                continue
            
            if in_string:
                current_statement.append(char)
                i += 1
                continue
            
            # Check for statement terminator (semicolon not in string or dollar quote)
            if char == ';' and not in_string and not in_dollar_quote:
                current_statement.append(char)
                statement = ''.join(current_statement).strip()
                if statement and not statement.isspace() and not statement.startswith('--'):
                    statements.append(statement)
                current_statement = []
                i += 1
                continue
            
            current_statement.append(char)
            i += 1
        
        # Add any remaining statement
        if current_statement:
            statement = ''.join(current_statement).strip()
            if statement and not statement.isspace() and not statement.startswith('--'):
                statements.append(statement)
        
        return statements

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

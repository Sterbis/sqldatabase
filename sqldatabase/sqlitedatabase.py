import sqlite3
from pathlib import Path
from typing import Generic

from .sqldatabase import SQLDatabase, T
from .sqldatatype import SQLDataTypes
from .sqltable import SQLTable
from .sqltranspiler import ESQLDialect


class SQLiteDataTypes(SQLDataTypes):
    """Represents SQLite-specific data types."""


class SQLiteDatabase(SQLDatabase[T], Generic[T]):
    """Represents a SQLite database.

    Attributes:
        dialect (ESQLDialect): The SQL dialect used by the database.
        path (Path): The file path to the SQLite database.
    """

    dialect = ESQLDialect.SQLITE

    def __init__(self, path: str | Path, autocommit: bool = False):
        """Initialize a SQLiteDatabase instance.

        Args:
            path (str | Path): The file path to the SQLite database.
            autocommit (bool, optional): Whether to enable autocommit mode. Defaults to False.
        """
        self.path = Path(path)
        connection = sqlite3.connect(self.path, autocommit=autocommit)
        SQLDatabase.__init__(self, "main", connection)

    def _parse_table_fully_qualified_name(
        self,
        table_fully_qualified_name: str,
    ) -> tuple[str | None, str | None, str | None]:
        """Parse a fully qualified table name into its components.

        Args:
            table_fully_qualified_name (str): The fully qualified table name.

        Returns:
            tuple[str | None, str | None, str | None]: A tuple containing the database name, schema name, and table name.
        """
        database_name = schema_name = table_name = None
        table_fully_qualified_name_parts = table_fully_qualified_name.split(".")
        if len(table_fully_qualified_name_parts) == 1:
            table_name = table_fully_qualified_name_parts[0]
        elif len(table_fully_qualified_name_parts) == 2:
            database_name, table_name = table_fully_qualified_name_parts
        else:
            assert (
                False
            ), f"Unexpected table fully qualified name: {table_fully_qualified_name}"

        return database_name, schema_name, table_name

    def get_table_fully_qualified_name(self, table: SQLTable) -> str:
        """Get the fully qualified name of a table.

        Args:
            table (SQLTable): The table for which to get the fully qualified name.

        Returns:
            str: The fully qualified name of the table.
        """
        if self.attached_databases:
            return f"{self.name}.{table.name}"
        else:
            return table.name

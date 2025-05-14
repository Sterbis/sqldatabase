from __future__ import annotations

import copy
import pprint
import sqlite3
import textwrap
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

import pyodbc  # type: ignore

from .sqlbase import SQLBase
from .sqlcolumn import SQLColumn
from .sqlcondition import SQLCondition
from .sqldatatype import SQLDataType, SQLDataTypeWithParameter
from .sqlfunction import SQLFunction, SQLFunctions
from .sqljoin import SQLJoin
from .sqlrecord import SQLRecord
from .sqlstatement import (
    ESQLOrderByType,
    SQLCreateTableStatement,
    SQLDeleteStatement,
    SQLDropTableStatement,
    SQLInsertIntoStatement,
    SQLSelectStatement,
    SQLStatement,
    SQLUpdateStatement,
)
from .sqltable import SQLTable, SQLTables
from .sqltranspiler import ESQLDialect

T = TypeVar("T", bound=SQLTables)


class SQLDatabase(SQLBase, Generic[T]):
    """
    Represents a SQL database.

    Attributes:
        dialect (ESQLDialect): The SQL dialect used by the database.
        tables (T): The tables in the database.
        default_schema_name (str | None): The default schema name for the database.
    """

    dialect: ESQLDialect
    tables: T
    default_schema_name: str | None = None

    def __init__(
        self,
        name: str,
        connection: sqlite3.Connection | pyodbc.Connection,
        tables: T | None = None,
    ) -> None:
        """
        Initialize a SQLDatabase instance.

        Args:
            name (str): The name of the database.
            connection (sqlite3.Connection | pyodbc.Connection): The database connection.
            tables (T | None, optional): The tables in the database. Defaults to None.
        """
        self.name = name
        self._connection = connection
        if tables is None:
            assert hasattr(self.__class__, "tables"), (
                "Database tables must be specified either as class attribute"
                "or passed when instance is created."
            )
            self.tables = self.__class__.tables
        else:
            self.tables = tables
        self.functions = SQLFunctions()
        self.attached_databases: dict[str, SQLDatabase] = {}
        data_types = {}
        for table in self.tables:
            table.database = self
            for column in table.columns:
                assert isinstance(
                    column.data_type, SQLDataType
                ), f"Unexpected data type: {column.data_type}"
                if isinstance(column.data_type, SQLDataTypeWithParameter):
                    column.data_type.database = self
                else:
                    if column.data_type.name not in data_types:
                        data_type = copy.deepcopy(column.data_type)
                        data_type.database = self
                        data_types[data_type.name] = data_type
                    column.data_type = data_types[column.data_type.name]

    @property
    def autocommit(self) -> bool:
        """
        Check if autocommit mode is enabled for the database.

        Returns:
            bool: True if autocommit is enabled, False otherwise.
        """
        return bool(self._connection.autocommit)

    @abstractmethod
    def _parse_table_fully_qualified_name(
        self,
        table_fully_qualified_name: str,
    ) -> tuple[str | None, str | None, str | None]:
        """
        Parse a fully qualified table name into its components.

        Args:
            table_fully_qualified_name (str): The fully qualified table name.

        Returns:
            tuple[str | None, str | None, str | None]: The database name, schema name, and table name.
        """

    @abstractmethod
    def get_table_fully_qualified_name(self, table: SQLTable) -> str:
        """
        Get the fully qualified name of a table.

        Args:
            table (SQLTable): The table to get the fully qualified name for.

        Returns:
            str: The fully qualified name of the table.
        """

    def _fetch_records(self, cursor: sqlite3.Cursor | pyodbc.Cursor) -> list[SQLRecord]:
        """
        Fetch records from a database cursor.

        Args:
            cursor (sqlite3.Cursor | pyodbc.Cursor): The database cursor.

        Returns:
            list[SQLRecord]: The fetched records.
        """
        records: list[SQLRecord] = []
        if cursor.description is not None:
            aliases = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                records.append(SQLRecord.from_database_row(aliases, row, self))
        return records

    def _fetch_ids(self, cursor: sqlite3.Cursor | pyodbc.Cursor) -> list[int] | None:
        """
        Fetch IDs from a database cursor.

        Args:
            cursor (sqlite3.Cursor | pyodbc.Cursor): The database cursor.

        Returns:
            list[int] | None: The fetched IDs, or None if no IDs are found.
        """
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None

    def _execute_statement(
        self, statement: SQLStatement
    ) -> sqlite3.Cursor | pyodbc.Cursor:
        """
        Execute a SQL statement.

        Args:
            statement (SQLStatement): The SQL statement to execute.

        Returns:
            sqlite3.Cursor | pyodbc.Cursor: The database cursor after execution.
        """
        return self.execute(statement.sql, statement.parameters)

    def to_sql(self) -> str:
        """
        Convert the database to its SQL representation.

        Returns:
            str: The SQL representation of the database.
        """
        return self.name

    def execute(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence = (),
    ) -> sqlite3.Cursor | pyodbc.Cursor:
        """
        Execute a raw SQL query.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict[str, Any] | Sequence, optional): The parameters for the query. Defaults to ().

        Returns:
            sqlite3.Cursor | pyodbc.Cursor: The database cursor after execution.
        """
        print("=" * 80)
        print("Executing SQL:")
        print("-" * 80)
        print(textwrap.indent(sql, "  "))
        print()
        print(
            textwrap.indent(
                f"parameters = {pprint.pformat(parameters, sort_dicts=False)}", "  "
            )
        )
        print()
        return self._connection.execute(sql, parameters)

    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        self._connection.commit()

    def rollback(self) -> None:
        """
        Roll back the current transaction.
        """
        self._connection.rollback()

    def close(self) -> None:
        """
        Close the database connection.
        """
        self._connection.close()

    def create_table(self, table: SQLTable, if_not_exists: bool = False) -> None:
        """
        Create a table in the database.

        Args:
            table (SQLTable): The table to create.
            if_not_exists (bool, optional): Whether to skip creation if the table already exists. Defaults to False.
        """
        self._execute_statement(
            SQLCreateTableStatement(self.dialect, table, if_not_exists)
        )

    def create_all_tables(self, if_not_exists: bool = False) -> None:
        """
        Create all tables in the database.

        Args:
            if_not_exists (bool, optional): Whether to skip creation if the tables already exist. Defaults to False.
        """
        for table in self.tables:
            self.create_table(table, if_not_exists)
        self.commit()

    def drop_table(self, table: SQLTable, if_exists: bool = False) -> None:
        """
        Drop a table from the database.

        Args:
            table (SQLTable): The table to drop.
            if_exists (bool, optional): Whether to skip dropping if the table does not exist. Defaults to False.
        """
        self._execute_statement(SQLDropTableStatement(self.dialect, table, if_exists))

    def drop_all_tables(self, if_exists: bool = False) -> None:
        """
        Drop all tables from the database.

        Args:
            if_exists (bool, optional): Whether to skip dropping if the tables do not exist. Defaults to False.
        """
        unsorted_tables = set(self.tables)
        sorted_tables: list[SQLTable] = []

        while unsorted_tables:
            referenced_tables = set()
            for table in unsorted_tables:
                for referenced_table in table.referenced_tables:
                    referenced_tables.add(referenced_table)
            unreferenced_tables = set(unsorted_tables) - set(referenced_tables)
            sorted_tables.extend(unreferenced_tables)
            unsorted_tables = referenced_tables

        for table in sorted_tables:
            self.drop_table(table, if_exists)

        self.commit()

    def get_table(self, table_fully_qualified_name: str) -> SQLTable:
        """
        Get a table by its fully qualified name.

        Args:
            table_fully_qualified_name (str): The fully qualified name of the table.

        Returns:
            SQLTable: The table with the specified name.

        Raises:
            AssertionError: If the table is not found.
        """
        database_name, schema_name, table_name = self._parse_table_fully_qualified_name(
            table_fully_qualified_name
        )
        if database_name is not None and database_name != self.name:
            database = self.attached_databases[database_name]
        else:
            database = self
        for table in database.tables:
            if table.name == table_name and table.schema_name == schema_name:
                return table
        assert (
            False
        ), f"Table '{table_name}', schema '{schema_name}', not found in database '{database.name}'."

    def insert_records(
        self,
        table: SQLTable,
        records: SQLRecord | Sequence[SQLRecord],
    ) -> list[int] | None:
        """
        Insert records into a table.

        Args:
            table (SQLTable): The table to insert records into.
            records (SQLRecord | Sequence[SQLRecord]): The records to insert.

        Returns:
            list[int] | None: The IDs of the inserted records, or None if no IDs are generated.
        """
        if isinstance(records, SQLRecord):
            records = [records]

        insert_statement = SQLInsertIntoStatement(self.dialect, table, records[0])
        ids = []
        for index, record in enumerate(records):
            if index > 0 and insert_statement.template_parameters is not None:
                for parameter, value in zip(
                    insert_statement.template_parameters.keys(), record.values()
                ):
                    insert_statement.template_parameters[parameter] = value
            cursor = self._execute_statement(insert_statement)
            if cursor.description is not None:
                row = cursor.fetchone()
                if row is not None:
                    ids.append(row[0])
        return ids if len(ids) else None

    def select_records(
        self,
        table: SQLTable,
        *items: SQLColumn | SQLFunction,
        where_condition: SQLCondition | None = None,
        joins: list[SQLJoin] | None = None,
        group_by_columns: list[SQLColumn] | None = None,
        having_condition: SQLCondition | None = None,
        order_by_items: list[SQLColumn | SQLFunction | ESQLOrderByType] | None = None,
        distinct: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SQLRecord]:
        """
        Select records from a table.

        Args:
            table (SQLTable): The table to select records from.
            *items (SQLColumn | SQLFunction): The columns or aggregate functions to select.
            where_condition (SQLCondition | None, optional): The condition to filter the records. Defaults to None.
            joins (list[SQLJoin] | None, optional): The joins to apply. Defaults to None.
            group_by_columns (list[SQLColumn] | None, optional): The columns to group by. Defaults to None.
            having_condition (SQLCondition | None, optional): The condition to filter the groups. Defaults to None.
            order_by_items (list[SQLColumn | SQLFunction | ESQLOrderByType] | None, optional): The items to order by. Defaults to None.
            distinct (bool, optional): Whether to select distinct records. Defaults to False.
            limit (int | None, optional): The maximum number of records to return. Defaults to None.
            offset (int | None, optional): The number of records to skip. Defaults to None.

        Returns:
            list[SQLRecord]: The selected records.
        """
        select_statement = SQLSelectStatement(
            self.dialect,
            table,
            *items,
            where_condition=where_condition,
            joins=joins,
            group_by_columns=group_by_columns,
            having_condition=having_condition,
            order_by_items=order_by_items,
            distinct=distinct,
            limit=limit,
            offset=offset,
        )
        cursor = self._execute_statement(select_statement)
        return self._fetch_records(cursor)

    def update_records(
        self,
        table: SQLTable,
        record: SQLRecord,
        where_condition: SQLCondition,
    ) -> list[int] | None:
        """
        Update records in a table.

        Args:
            table (SQLTable): The table to update records in.
            record (SQLRecord): The record with updated values.
            where_condition (SQLCondition): The condition to filter the records to update.

        Returns:
            list[int] | None: The IDs of the updated records, or None if no IDs are generated.
        """
        update_statement = SQLUpdateStatement(
            self.dialect,
            table,
            record,
            where_condition,
        )
        cursor = self._execute_statement(update_statement)
        return self._fetch_ids(cursor)

    def delete_records(
        self,
        table: SQLTable,
        where_condition: SQLCondition,
    ) -> list[int] | None:
        """
        Delete records from a table.

        Args:
            table (SQLTable): The table to delete records from.
            where_condition (SQLCondition): The condition to filter the records to delete.

        Returns:
            list[int] | None: The IDs of the deleted records, or None if no IDs are generated.
        """
        delete_statement = SQLDeleteStatement(self.dialect, table, where_condition)
        cursor = self._execute_statement(delete_statement)
        return self._fetch_ids(cursor)

    def record_count(self, table: SQLTable) -> int:
        """
        Get the count of records in a table.

        Args:
            table (SQLTable): The table to count records in.

        Returns:
            int: The count of records in the table.
        """
        count_function = self.functions.COUNT()
        records = self.select_records(table, count_function)
        return records[0][count_function]

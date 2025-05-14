from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader

from .sqlbase import SQLBase, SQLBaseEnum
from .sqlcolumn import SQLColumn
from .sqlfunction import SQLFunction
from .sqltranspiler import ESQLDialect, SQLTranspiler

if TYPE_CHECKING:
    from .sqlcondition import SQLCondition
    from .sqljoin import SQLJoin
    from .sqlrecord import SQLRecord
    from .sqltable import SQLTable


class ESQLOrderByType(SQLBaseEnum):
    """Enumeration for SQL ORDER BY types.

    Attributes:
        ASCENDING (str): Represents ascending order.
        DESCENDING (str): Represents descending order.
    """

    ASCENDING = "ASC"
    DESCENDING = "DESC"


class SQLStatement(SQLBase):
    """Represents a SQL statement.

    Attributes:
        dialect (ESQLDialect): The SQL dialect for the statement.
        context (dict): The context for rendering the statement.
        template_parameters (dict[str, Any]): Parameters for the statement template.
        template_sql (str): The rendered SQL template.
    """

    _environment = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates")
    )
    template_dialect = ESQLDialect.SQLITE
    template_file: str

    def __init__(
        self,
        dialect: ESQLDialect,
        parameters: dict[str, Any] | None = None,
        **context,
    ) -> None:
        """Initialize a SQLStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            parameters (dict[str, Any] | None, optional): Parameters for the statement template. Defaults to None.
            context (dict): Additional context for rendering the statement.
        """
        self.dialect = dialect
        self.context = context
        self.context["dialect"] = dialect.value
        self.context["parameters"] = parameters
        self.template_parameters = parameters or {}
        self.template_sql = self._render_template()

    @property
    def sql(self) -> str:
        """Get the SQL representation of the statement.

        Returns:
            str: The SQL representation of the statement.
        """
        return SQLTranspiler(self.dialect).transpile_sql(
            self.template_sql,
            self.template_dialect,
            pretty=True,
        )

    @property
    def parameters(self) -> dict[str, Any] | Sequence:
        """Get the parameters for the statement.

        Returns:
            dict[str, Any] | Sequence: The parameters for the statement.
        """
        return SQLTranspiler(self.dialect).transpile_parameters(
            self.template_sql,
            self.template_parameters,
        )

    def _render_template(self) -> str:
        """Render the SQL template.

        Returns:
            str: The rendered SQL template.
        """
        template = self._environment.get_template(self.template_file)
        template_sql = template.render(self.context)
        template_sql = "\n".join(
            line for line in template_sql.splitlines() if line.strip()
        )
        return template_sql

    def to_sql(self) -> str:
        """Get the SQL representation of the statement.

        Returns:
            str: The SQL representation of the statement.
        """
        return self.sql


class SQLCreateTableStatement(SQLStatement):
    """Represents a SQL CREATE TABLE statement."""

    template_file = "create_table_statement.sql.j2"

    def __init__(
        self, dialect: ESQLDialect, table: SQLTable, if_not_exists: bool = False
    ) -> None:
        """Initialize a SQLCreateTableStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to create.
            if_not_exists (bool, optional): Whether to include IF NOT EXISTS. Defaults to False.
        """
        SQLStatement.__init__(self, dialect, table=table, if_not_exists=if_not_exists)


class SQLDropTableStatement(SQLStatement):
    """Represents a SQL DROP TABLE statement."""

    template_file = "drop_table_statement.sql.j2"

    def __init__(
        self, dialect: ESQLDialect, table: SQLTable, if_exists: bool = False
    ) -> None:
        """Initialize a SQLDropTableStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to drop.
            if_exists (bool, optional): Whether to include IF EXISTS. Defaults to False.
        """
        SQLStatement.__init__(self, dialect, table=table, if_exists=if_exists)


class SQLInsertIntoStatement(SQLStatement):
    """Represents a SQL INSERT INTO statement."""

    template_file = "insert_into_statement.sql.j2"

    def __init__(
        self,
        dialect: ESQLDialect,
        table: SQLTable,
        record: SQLRecord,
    ) -> None:
        """Initialize a SQLInsertIntoStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to insert into.
            record (SQLRecord): The record to insert.
        """
        parameters = record.to_database_parameters()
        columns = list(record.keys())
        SQLStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            columns=columns,
        )


class SQLSelectStatement(SQLStatement):
    """Represents a SQL SELECT statement."""

    template_file = "select_statement.sql.j2"

    def __init__(
        self,
        dialect: ESQLDialect,
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
        is_subquery: bool = False,
    ) -> None:
        """Initialize a SQLSelectStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to select from.
            *items (SQLColumn | SQLFunction): The columns or aggregate functions to select.
            where_condition (SQLCondition | None, optional): The WHERE condition. Defaults to None.
            joins (list[SQLJoin] | None, optional): The JOIN clauses. Defaults to None.
            group_by_columns (list[SQLColumn] | None, optional): The GROUP BY columns. Defaults to None.
            having_condition (SQLCondition | None, optional): The HAVING condition. Defaults to None.
            order_by_items (list[SQLColumn | SQLFunction | ESQLOrderByType] | None, optional): The ORDER BY items. Defaults to None.
            distinct (bool, optional): Whether to include DISTINCT. Defaults to False.
            limit (int | None, optional): The LIMIT value. Defaults to None.
            offset (int | None, optional): The OFFSET value. Defaults to None.
            is_subquery (bool, optional): Whether the statement is a subquery. Defaults to False.
        """
        parameters = {}
        if where_condition:
            parameters.update(where_condition.parameters)
        if having_condition:
            parameters.update(having_condition.parameters)
        preprocessed_items = self._preprocess_items(table, *items)
        preprocessed_order_by_items = self._preprocess_order_by_items(order_by_items)

        SQLStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            items=preprocessed_items,
            where_condition=where_condition,
            joins=joins,
            group_by_columns=group_by_columns,
            having_condition=having_condition,
            order_by_items=preprocessed_order_by_items,
            distinct=distinct,
            limit=limit,
            offset=offset,
            is_subquery=is_subquery,
        )

    @staticmethod
    def _preprocess_items(
        table: SQLTable, *items: SQLColumn | SQLFunction
    ) -> list[SQLColumn | SQLFunction]:
        """Preprocess the items to select.

        Args:
            table (SQLTable): The table to select from.
            *items (SQLColumn | SQLFunction): The columns or aggregate functions to select.

        Returns:
            list[SQLColumn | SQLFunction]: The preprocessed items.
        """
        if len(items) == 0:
            preprocessed_items = list(table.columns)
        else:
            preprocessed_items = list(items)
        return preprocessed_items

    @staticmethod
    def _preprocess_order_by_items(
        order_by_items: list[SQLColumn | SQLFunction | ESQLOrderByType] | None,
    ) -> list[tuple[SQLColumn | SQLFunction, ESQLOrderByType | None]] | None:
        """Preprocess the ORDER BY items.

        Args:
            order_by_items (list[SQLColumn | SQLFunction | ESQLOrderByType] | None): The ORDER BY items.

        Returns:
            list[tuple[SQLColumn | SQLFunction, ESQLOrderByType | None]] | None: The preprocessed ORDER BY items.
        """
        if order_by_items is not None:
            preprocessed_order_by_items: list[
                tuple[SQLColumn | SQLFunction, ESQLOrderByType | None]
            ] = []
            index = 0
            while index < len(order_by_items):
                item = order_by_items[index]
                assert isinstance(
                    item, (SQLColumn, SQLFunction)
                ), f"Unexpected order by item type {type(item)} with index {index}."
                order = None
                if index + 1 < len(order_by_items):
                    next_item = order_by_items[index + 1]
                    if isinstance(next_item, ESQLOrderByType):
                        order = next_item
                        index += 1
                preprocessed_order_by_items.append((item, order))
                index += 1
            return preprocessed_order_by_items
        return None

    def generate_parameter_name(self) -> str:
        """Generate a unique parameter name for the SELECT statement.

        Returns:
            str: The generated parameter name.
        """
        assert (
            len(self.context["items"]) == 1
        ), "Select statement must return exactly one value when compared with parameter value."
        return f"SELECT_{self.context['items'][0].generate_parameter_name()}"


class SQLUpdateStatement(SQLStatement):
    """Represents a SQL UPDATE statement."""

    template_file = "update_statement.sql.j2"

    def __init__(
        self,
        dialect: ESQLDialect,
        table: SQLTable,
        record: SQLRecord,
        where_condition: SQLCondition,
    ) -> None:
        """Initialize a SQLUpdateStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to update.
            record (SQLRecord): The record with updated values.
            where_condition (SQLCondition): The WHERE condition.
        """
        parameters = record.to_database_parameters()
        parameters.update(where_condition.parameters)
        columns_and_parameters = list(zip(record.keys(), parameters))
        SQLStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            columns_and_parameters=columns_and_parameters,
            where_condition=where_condition,
        )


class SQLDeleteStatement(SQLStatement):
    """Represents a SQL DELETE statement."""

    template_file = "delete_statement.sql.j2"

    def __init__(
        self,
        dialect: ESQLDialect,
        table: SQLTable,
        where_condition: SQLCondition,
    ) -> None:
        """Initialize a SQLDeleteStatement instance.

        Args:
            dialect (ESQLDialect): The SQL dialect for the statement.
            table (SQLTable): The table to delete from.
            where_condition (SQLCondition): The WHERE condition.
        """
        parameters = where_condition.parameters
        SQLStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            where_condition=where_condition,
        )

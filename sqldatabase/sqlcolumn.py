from __future__ import annotations

import copy
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeContainer

from .sqlbase import SQLBase, value_to_sql
from .sqldatatype import SQLDataType, SQLDataTypes
from .sqlfilter import SQLFilters

if TYPE_CHECKING:
    from .sqltable import SQLTable


class SQLColumn(SQLBase):
    """
    Represents a column in a SQL table.

    Attributes:
        name (str): The name of the column.
        data_type (SQLDataType): The data type of the column.
        primary_key (bool): Whether the column is a primary key.
        autoincrement (bool): Whether the column is auto-incremented.
        not_null (bool): Whether the column is NOT NULL.
        unique (bool): Whether the column is unique.
        default (Any): The default value of the column.
        reference (SQLColumn | None): A reference to another column.
        to_database_converter (Callable[[Any], Any] | None): Function to convert values to database format.
        from_database_converter (Callable[[Any], Any] | None): Function to convert values from database format.
        values (type[Enum] | None): Enum values for the column.
    """

    def __init__(
        self,
        name: str,
        data_type: SQLDataType,
        primary_key: bool = False,
        autoincrement: bool = False,
        not_null: bool = False,
        unique: bool = False,
        default_value: Any = None,
        reference: SQLColumn | None = None,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
        values: type[Enum] | None = None,
    ):
        """
        Initialize a SQLColumn instance.

        Args:
            name (str): The name of the column.
            data_type (SQLDataType): The data type of the column.
            primary_key (bool, optional): Whether the column is a primary key. Defaults to False.
            autoincrement (bool, optional): Whether the column is auto-incremented. Defaults to False.
            not_null (bool, optional): Whether the column is NOT NULL. Defaults to False.
            unique (bool, optional): Whether the column is unique. Defaults to False.
            default (Any, optional): The default value of the column. Defaults to None.
            reference (SQLColumn | None, optional): A reference to another column. Defaults to None.
            to_database_converter (Callable[[Any], Any] | None, optional): Function to convert values to database format. Defaults to None.
            from_database_converter (Callable[[Any], Any] | None, optional): Function to convert values from database format. Defaults to None.
            values (type[Enum] | None, optional): Enum values for the column. Defaults to None.
        """
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.autoincrement = autoincrement
        self.not_null = not_null
        self.unique = unique
        self.default_value = default_value
        self.reference = reference
        self.values = values
        if self.values is None:
            self.to_database_converter = to_database_converter
            self.from_database_converter = from_database_converter
        else:
            assert (
                to_database_converter is None and from_database_converter is None
            ), "Converters cannot be specified together with values."
            self.to_database_converter = lambda value: value.value
            self.from_database_converter = self.values
        self.filters = SQLFilters(self)
        self._foreign_keys: list[SQLColumn] = []
        if self.reference is not None:
            self.reference._foreign_keys.append(self)
        self.table: SQLTable | None = None

    def __deepcopy__(self, memo) -> SQLColumn:
        """Create a deep copy of the SQLColumn instance.

        Args:
            memo (dict): A dictionary to keep track of already copied objects.

        Returns:
            SQLColumn: A deep copy of the current SQLColumn instance.
        """
        if id(self) in memo:
            return memo[id(self)]

        cls = self.__class__
        column = cls.__new__(cls)
        memo[id(self)] = column
        for name, value in self.__dict__.items():
            if name not in ("_foreign_keys", "filters", "reference", "table"):
                setattr(column, name, copy.deepcopy(value, memo))

        column.filters = SQLFilters(column)
        column.reference = self.reference
        if column.reference is not None:
            column.reference._foreign_keys.remove(self)
            column.reference._foreign_keys.append(column)
        column._foreign_keys = copy.copy(self._foreign_keys)
        for foreign_key in column._foreign_keys:
            foreign_key.reference = column
        return column

    @property
    def alias(self) -> str:
        return f"COLUMN.{self.fully_qualified_name}"

    @property
    def fully_qualified_name(self) -> str:
        if self.table is None:
            raise AttributeError("The 'table' attribute is not set for this column.")
        return f"{self.table.fully_qualified_name}.{self.name}"

    def generate_parameter_name(self) -> str:
        """Generate a unique parameter name for the column.

        Returns:
            str: A unique parameter name in the format '<fully_qualified_name>_<uuid>'.
        """
        return f"{self.fully_qualified_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"

    def to_sql(self) -> str:
        """Convert the column to its SQL representation.

        Returns:
            str: The SQL representation of the column name.
        """
        return self.fully_qualified_name

    def default_value_to_sql(self) -> str:
        """Convert the default value of the column to its SQL representation.

        Returns:
            str: The SQL representation of the default value.
        """
        value = self.default_value
        if self.to_database_converter is not None:
            value = self.to_database_converter(value)
        if self.data_type.to_database_converter is not None:
            value = self.data_type.to_database_converter(value)
        return value_to_sql(value)


class SQLColumns(EnumLikeContainer[SQLColumn]):
    """Container for managing multiple SQLColumn instances."""

    item_type = SQLColumn


class SQLColumnsWithID(SQLColumns):
    """Specialized SQLColumns container with a predefined 'ID' column."""

    ID = SQLColumn("id", SQLDataTypes.INTEGER, primary_key=True, autoincrement=True)

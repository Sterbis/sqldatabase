from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeMixedContainer

from .sqlbase import SQLBase

if TYPE_CHECKING:
    from .sqldatabase import SQLDatabase


class SQLDataType(SQLBase):
    """
    Represents a SQL data type.

    Attributes:
        name (str): The name of the data type.
        type (type): The Python type corresponding to the SQL data type.
        to_database_converter (Callable[[Any], Any] | None): Function to convert values to database format.
        from_database_converter (Callable[[Any], Any] | None): Function to convert values from database format.
    """

    database: SQLDatabase

    def __init__(
        self,
        name: str,
        type_: type,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
    ) -> None:
        """
        Initialize a SQLDataType instance.

        Args:
            name (str): The name of the data type.
            type_ (type): The Python type corresponding to the SQL data type.
            to_database_converter (Callable[[Any], Any] | None, optional): Function to convert values to database format. Defaults to None.
            from_database_converter (Callable[[Any], Any] | None, optional): Function to convert values from database format. Defaults to None.
        """
        self.name = name
        self.type = type_
        self.to_database_converter = to_database_converter
        self.from_database_converter = from_database_converter

    def to_sql(self) -> str:
        """
        Convert the data type to its SQL representation.

        Returns:
            str: The SQL representation of the data type.
        """
        return self.name


class SQLDataTypeWithParameter(SQLDataType):
    """Represents a SQL data type with an additional parameter.

    Attributes:
        parameter (Any): The parameter associated with the data type.
    """

    def __init__(
        self,
        name: str,
        type_: type,
        parameter: Any,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
    ) -> None:
        """Initialize a SQLDataTypeWithParameter instance.

        Args:
            name (str): The name of the data type.
            type_ (type): The Python type corresponding to the SQL data type.
            parameter (Any): The parameter associated with the data type.
            to_database_converter (Callable[[Any], Any] | None, optional): Function to convert values to database format. Defaults to None.
            from_database_converter (Callable[[Any], Any] | None, optional): Function to convert values from database format. Defaults to None.
        """
        SQLDataType.__init__(
            self, name, type_, to_database_converter, from_database_converter
        )
        self.parameter = parameter

    def to_sql(self) -> str:
        """Convert the data type with parameter to its SQL representation.

        Returns:
            str: The SQL representation of the data type with parameter.
        """
        return f"{self.name}({self.parameter})"


class SQLIntegerDataType(SQLDataType):
    """Represents the SQL INTEGER data type."""

    def __init__(self) -> None:
        """Initialize a SQLIntegerDataType instance."""
        SQLDataType.__init__(self, "INTEGER", int)


class SQLFloatDataType(SQLDataType):
    """Represents the SQL REAL (float) data type."""

    def __init__(self) -> None:
        """Initialize a SQLFloatDataType instance."""
        SQLDataType.__init__(self, "REAL", float)


class SQLTextDataType(SQLDataType):
    """Represents the SQL TEXT data type."""

    def __init__(self) -> None:
        """Initialize a SQLTextDataType instance."""
        SQLDataType.__init__(self, "TEXT", str)

    def to_sql(self) -> str:
        """Convert the TEXT data type to its SQL representation.

        Returns:
            str: The SQL representation of the TEXT data type.
        """
        from .sqlserverdatabase import SQLServerDatabase

        return (
            "NVARCHAR(255)"
            if isinstance(self.database, SQLServerDatabase)
            else self.name
        )


class SQLBlobDataType(SQLDataType):
    """Represents the SQL BLOB data type."""

    def __init__(self) -> None:
        """Initialize a SQLBlobDataType instance."""
        SQLDataType.__init__(self, "BLOB", bytes)


class SQLBooleanDataType(SQLDataType):
    """Represents the SQL BOOLEAN data type."""

    def __init__(self) -> None:
        """Initialize a SQLBooleanDataType instance."""
        SQLDataType.__init__(
            self, "BOOLEAN", bool, self._to_database_value, self._from_database_value
        )

    def _to_database_value(self, value: bool) -> bool | int:
        """Convert a boolean value to its database representation.

        Args:
            value (bool): The boolean value to convert.

        Returns:
            bool | int: The database representation of the boolean value.
        """
        from .sqlitedatabase import SQLiteDatabase

        return int(value) if isinstance(self.database, SQLiteDatabase) else value

    def _from_database_value(self, value: bool | int) -> bool:
        """Convert a database value to a boolean.

        Args:
            value (bool | int): The database value to convert.

        Returns:
            bool: The boolean representation of the value.
        """
        return bool(value)

    def to_sql(self) -> str:
        """Convert the BOOLEAN data type to its SQL representation.

        Returns:
            str: The SQL representation of the BOOLEAN data type.
        """
        from .sqlitedatabase import SQLiteDatabase

        return "INTEGER" if isinstance(self.database, SQLiteDatabase) else self.name


class SQLDateDataType(SQLDataType):
    """Represents the SQL DATE data type."""

    def __init__(self) -> None:
        """Initialize a SQLDateDataType instance."""
        SQLDataType.__init__(
            self,
            "DATE",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.date) -> datetime.date | str:
        """Convert a date value to its database representation.

        Args:
            value (datetime.date): The date value to convert.

        Returns:
            datetime.date | str: The database representation of the date value.
        """
        from .sqlitedatabase import SQLiteDatabase

        return value.isoformat() if isinstance(self.database, SQLiteDatabase) else value

    def _from_database_value(self, value: datetime.date | str) -> datetime.date:
        """Convert a database value to a date.

        Args:
            value (datetime.date | str): The database value to convert.

        Returns:
            datetime.date: The date representation of the value.
        """
        return datetime.date.fromisoformat(value) if isinstance(value, str) else value

    def to_sql(self) -> str:
        """Convert the DATE data type to its SQL representation.

        Returns:
            str: The SQL representation of the DATE data type.
        """
        from .sqlitedatabase import SQLiteDatabase

        return "TEXT" if isinstance(self.database, SQLiteDatabase) else self.name


class SQLTimeDataType(SQLDataType):
    """Represents the SQL TIME data type."""

    def __init__(self) -> None:
        """Initialize a SQLTimeDataType instance."""
        SQLDataType.__init__(
            self,
            "TIME",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.time) -> datetime.time | str:
        """Convert a time value to its database representation.

        Args:
            value (datetime.time): The time value to convert.

        Returns:
            datetime.time | str: The database representation of the time value.
        """
        from .sqlitedatabase import SQLiteDatabase

        return value.isoformat() if isinstance(self.database, SQLiteDatabase) else value

    def _from_database_value(self, value: datetime.time | str) -> datetime.time:
        """Convert a database value to a time.

        Args:
            value (datetime.time | str): The database value to convert.

        Returns:
            datetime.time: The time representation of the value.
        """
        return datetime.time.fromisoformat(value) if isinstance(value, str) else value

    def to_sql(self) -> str:
        """Convert the TIME data type to its SQL representation.

        Returns:
            str: The SQL representation of the TIME data type.
        """
        from .sqlitedatabase import SQLiteDatabase

        return "TEXT" if isinstance(self.database, SQLiteDatabase) else self.name


class SQLDateTimeDataType(SQLDataType):
    """Represents the SQL DATETIME data type."""

    def __init__(self) -> None:
        """Initialize a SQLDateTimeDataType instance."""
        SQLDataType.__init__(
            self,
            "DATETIME",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.datetime) -> datetime.datetime | str:
        """Convert a datetime value to its database representation.

        Args:
            value (datetime.datetime): The datetime value to convert.

        Returns:
            datetime.datetime | str: The database representation of the datetime value.
        """
        from .sqlitedatabase import SQLiteDatabase

        return value.isoformat() if isinstance(self.database, SQLiteDatabase) else value

    def _from_database_value(self, value: datetime.datetime | str) -> datetime.datetime:
        """Convert a database value to a datetime.

        Args:
            value (datetime.datetime | str): The database value to convert.

        Returns:
            datetime.datetime: The datetime representation of the value.
        """
        return (
            datetime.datetime.fromisoformat(value) if isinstance(value, str) else value
        )

    def to_sql(self) -> str:
        """Convert the DATETIME data type to its SQL representation.

        Returns:
            str: The SQL representation of the DATETIME data type.
        """
        from .sqlitedatabase import SQLiteDatabase

        return "TEXT" if isinstance(self.database, SQLiteDatabase) else self.name


class SQLDataTypes(EnumLikeMixedContainer[SQLDataType]):
    """Container for managing multiple SQL data types."""

    item_type = SQLDataType

    BLOB = SQLBlobDataType()
    BOOLEAN = SQLBooleanDataType()
    DATE = SQLDateDataType()
    DATETIME = SQLDateTimeDataType()
    FLOAT = SQLFloatDataType()
    INTEGER = SQLIntegerDataType()
    TEXT = SQLTextDataType()
    TIME = SQLTimeDataType()

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeClassContainer

from .sqlbase import SQLBase
from .sqldatatype import SQLDataType

if TYPE_CHECKING:
    from .sqlcolumn import SQLColumn


class SQLFunction(SQLBase):
    """
    Represents a SQL function (e.g., COUNT, SUM).

    Attributes:
        name (str): The name of the function.
        column (SQLColumn | None): The column the function operates on.
    """

    name: str

    def __init__(self, column: SQLColumn | None = None):
        """
        Initialize a SQLFunction instance.

        Args:
            column (SQLColumn | None, optional): The column the function operates on. Defaults to None.
        """
        assert hasattr(
            self, "name"
        ), "Function name must be specified as class attribute."
        self.column = column

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SQLFunction)
            and self.fully_qualified_name == other.fully_qualified_name
        )

    def __hash__(self):
        return hash(self.fully_qualified_name)

    @property
    def alias(self) -> str:
        """
        Get the alias for the function.

        Returns:
            str: The alias for the function.
        """
        if self.column is None:
            return f"FUNCTION.{self.name}"
        else:
            return f"FUNCTION.{self.name}.{self.column.alias}"

    @property
    def fully_qualified_name(self) -> str:
        """
        Get the fully qualified name of the function.

        Returns:
            str: The fully qualified name of the function.
        """
        if self.column is None:
            return f"{self.name.upper()}(*)"
        else:
            return f"{self.name.upper()}({self.column.fully_qualified_name})"

    @property
    def to_database_converter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.to_database_converter
        return None

    @property
    def from_database_converter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.from_database_converter
        return None

    @property
    def data_type(self) -> SQLDataType | None:
        if self.column is not None:
            return self.column.data_type
        return None

    def generate_parameter_name(self) -> str:
        if self.column is None:
            return f"{self.name}_{uuid.uuid4().hex[:8]}"
        else:
            return f"{self.name}_{self.column.generate_parameter_name()}"

    def to_sql(self) -> str:
        if self.column is None:
            return f"{self.name.upper()}(*)"
        else:
            return f"{self.name.upper()}({self.column})"


class SQLCountFunction(SQLFunction):
    name = "count"


class SQLFunctionWithMandatoryColumn(SQLFunction):
    """Represents a SQL function that requires a column."""

    def __init__(self, column: SQLColumn):
        """Initialize a SQLFunctionWithMandatoryColumn instance.

        Args:
            column (SQLColumn): The column the function operates on.
        """
        SQLFunction.__init__(self, column)


class SQLMinFunction(SQLFunctionWithMandatoryColumn):
    """Represents the SQL MIN aggregate function."""

    name = "min"


class SQLMaxFunction(SQLFunctionWithMandatoryColumn):
    """Represents the SQL MAX aggregate function."""

    name = "max"


class SQLSumFunction(SQLFunctionWithMandatoryColumn):
    """Represents the SQL SUM aggregate function."""

    name = "sum"


class SQLAvgFunction(SQLFunctionWithMandatoryColumn):
    """Represents the SQL AVG aggregate function."""

    name = "avg"


class SQLFunctions(EnumLikeClassContainer[SQLFunction]):
    """Container for managing multiple SQL functions."""

    item_type = SQLFunction

    AVG = SQLAvgFunction
    COUNT = SQLCountFunction
    MAX = SQLMaxFunction
    MIN = SQLMinFunction
    SUM = SQLSumFunction

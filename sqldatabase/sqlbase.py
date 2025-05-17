from abc import ABC, abstractmethod
from enum import Enum
from typing import Any


class SQLBase(ABC):
    """
    Abstract base class for SQL-related objects.

    Methods:
        to_sql: Abstract method to convert the object to its SQL representation.
    """

    def __str__(self) -> str:
        return self.to_sql()

    @abstractmethod
    def to_sql(self) -> str:
        """
        Convert the object to its SQL representation.

        Returns:
            str: The SQL representation of the object.
        """


class SQLBaseEnum(Enum):
    """
    Base class for SQL-related enumerations.

    Methods:
        to_sql: Convert the enumeration value to its SQL representation.
    """

    def __str__(self) -> str:
        return self.to_sql()

    def to_sql(self) -> str:
        """
        Convert the enumeration value to its SQL representation.

        Returns:
            str: The SQL representation of the enumeration value.
        """
        return self.value


class ESQLDateTimeExpression(SQLBaseEnum):
    CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"
    CURRENT_DATE = "CURRENT_DATE"
    CURRENT_TIME = "CURRENT_TIME"


def value_to_sql(value: Any) -> str:
    """
    Convert a Python value to its SQL representation.

    Args:
        value (Any): The value to convert.

    Returns:
        str: The SQL representation of the value.
    """
    if isinstance(value, str):
        value = value.replace("'", "''")
        return f"'{value}'"
    elif isinstance(value, ESQLDateTimeExpression):
        return value.value
    elif value is None:
        return "NULL"
    else:
        return str(value)

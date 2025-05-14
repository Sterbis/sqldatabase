from __future__ import annotations

from typing import TYPE_CHECKING

from .sqlbase import SQLBase, SQLBaseEnum
from .sqlcondition import SQLCondition
from .sqloperator import ESQLComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SQLColumn
    from .sqltable import SQLTable


class ESQLJoinType(SQLBaseEnum):
    """Enumeration for SQL join types.

    Attributes:
        CROSS (str): Represents a CROSS JOIN.
        FULL (str): Represents a FULL OUTER JOIN.
        INNER (str): Represents an INNER JOIN.
        LEFT (str): Represents a LEFT OUTER JOIN.
        RIGHT (str): Represents a RIGHT OUTER JOIN.
    """

    CROSS = "CROSS"
    FULL = "FULL"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"


class SQLJoin(SQLBase):
    """Represents a SQL JOIN clause.

    Attributes:
        table (SQLTable): The table to join.
        type (ESQLJoinType): The type of join (e.g., INNER, LEFT).
        condition (SQLCondition): The condition for the join.
    """

    def __init__(
        self,
        table: SQLTable,
        *columns: SQLColumn,
        type_: ESQLJoinType = ESQLJoinType.INNER,
        operator: ESQLComparisonOperator = ESQLComparisonOperator.EQUAL,
    ) -> None:
        """Initialize a SQLJoin instance.

        Args:
            table (SQLTable): The table to join.
            columns (SQLColumn): The columns involved in the join condition.
            type_ (ESQLJoinType, optional): The type of join. Defaults to INNER.
            operator (ESQLComparisonOperator, optional): The comparison operator. Defaults to IS_EQUAL.
        """
        self.table = table
        self.type = type_
        self.condition = SQLCondition(columns[0], operator, *columns[1:])

    def to_sql(self) -> str:
        """Convert the join clause to its SQL representation.

        Returns:
            str: The SQL representation of the join clause.
        """
        return f"{self.type} JOIN {self.table.fully_qualified_name} ON {self.condition}"

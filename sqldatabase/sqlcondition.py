from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .sqlbase import SQLBase
from .sqlfunction import SQLFunction
from .sqloperator import ESQLComparisonOperator, ESQLLogicalOperator

if TYPE_CHECKING:
    from .sqlcolumn import SQLColumn
    from .sqlstatement import SQLSelectStatement


class SQLCondition(SQLBase):
    """
    Represents a SQL condition used in WHERE, HAVING, or JOIN clauses.

    Attributes:
        left (SQLColumn | SQLFunction | SQLSelectStatement): The left-hand side of the condition.
        operator (ESQLComparisonOperator): The comparison operator.
        right (Any): The right-hand side of the condition.
        parameters (dict[str, Any]): Parameters for the condition.
        _values_to_sql (list[str]): SQL representations of the values.
    """

    def __init__(
        self,
        left: SQLColumn | SQLFunction | SQLSelectStatement,
        operator: ESQLComparisonOperator,
        *right: Any,
    ) -> None:
        """
        Initialize a SQLCondition instance.

        Args:
            left (SQLColumn | SQLFunction | SQLSelectStatement): The left-hand side of the condition.
            operator (ESQLComparisonOperator): The comparison operator.
            right (Any): The right-hand side of the condition.
        """
        from .sqlstatement import SQLSelectStatement

        self.left = left
        self.operator = operator
        self.right = right
        self.parameters: dict[str, Any] = {}
        self._values_to_sql: list[str] = []
        self._validate_value_count()
        self._parse_values()
        if isinstance(self.left, SQLSelectStatement):
            self.parameters.update(self.left.parameters)

    def __and__(self, other: SQLCondition):
        """
        Combine this condition with another using the AND logical operator.

        Args:
            other (SQLCondition): The other condition to combine.

        Returns:
            SQLCompoundCondition: A new compound condition.
        """
        return SQLCompoundCondition(self, ESQLLogicalOperator.AND, other)

    def __or__(self, other: SQLCondition):
        """
        Combine this condition with another using the OR logical operator.

        Args:
            other (SQLCondition): The other condition to combine.

        Returns:
            SQLCompoundCondition: A new compound condition.
        """
        return SQLCompoundCondition(self, ESQLLogicalOperator.OR, other)

    def _validate_value_count(self) -> None:
        if self.operator in (
            ESQLComparisonOperator.IS_NULL,
            ESQLComparisonOperator.IS_NOT_NULL,
        ):
            required_value_count = 0
        elif self.operator in (
            ESQLComparisonOperator.BETWEEN,
            ESQLComparisonOperator.NOT_BETWEEN,
        ):
            required_value_count = 2
        elif self.operator in (
            ESQLComparisonOperator.IN,
            ESQLComparisonOperator.NOT_IN,
        ):
            required_value_count = None
        else:
            required_value_count = 1
        assert (
            required_value_count is None or len(self.right) == required_value_count
        ), (
            f"Expected exactly {required_value_count} values for {self.operator} operator,"
            f" {len(self.right)} were given."
        )

    def _parse_values(self) -> None:
        from .sqlcolumn import SQLColumn
        from .sqlrecord import SQLRecord
        from .sqlstatement import SQLSelectStatement

        item = (
            self.left.context["items"][0]
            if isinstance(self.left, SQLSelectStatement)
            else self.left
        )
        for value in self.right:
            if isinstance(value, SQLColumn):
                self._values_to_sql.append(value.fully_qualified_name)
            elif isinstance(value, SQLFunction):
                self._values_to_sql.append(value.to_sql())
            elif isinstance(value, SQLSelectStatement):
                self._values_to_sql.append(f"({value.template_sql.rstrip(";")})")
                self.parameters.update(value.template_parameters)
            else:
                parameter = self.left.generate_parameter_name()
                self._values_to_sql.append(f":{parameter}")
                self.parameters[parameter] = SQLRecord.to_database_value(item, value)

    def to_sql(self) -> str:
        from .sqlcolumn import SQLColumn
        from .sqlstatement import SQLSelectStatement

        if isinstance(self.left, SQLColumn):
            sql = self.left.fully_qualified_name
        elif isinstance(self.left, SQLFunction):
            sql = self.left.to_sql()
        elif isinstance(self.left, SQLSelectStatement):
            sql = f"({self.left.to_sql()})"
        else:
            assert False, f"Invalid item: {self.left}."

        sql += f" {self.operator}"
        if self.operator in (
            ESQLComparisonOperator.IS_NULL,
            ESQLComparisonOperator.IS_NOT_NULL,
        ):
            return sql
        elif self.operator in (
            ESQLComparisonOperator.BETWEEN,
            ESQLComparisonOperator.NOT_BETWEEN,
        ):
            lower_value, upper_value = self._values_to_sql
            return sql + f" {lower_value} AND {upper_value}"
        elif self.operator in (
            ESQLComparisonOperator.IN,
            ESQLComparisonOperator.NOT_IN,
        ):
            return sql + f" ({', '.join(self._values_to_sql)})"
        else:
            return sql + f" {self._values_to_sql[0]}"


class SQLCompoundCondition(SQLCondition):
    """Represents a compound SQL condition combining two conditions with a logical operator.

    Attributes:
        left (SQLCondition): The left-hand side condition.
        operator (ESQLLogicalOperator): The logical operator (e.g., AND, OR).
        right (SQLCondition): The right-hand side condition.
        parameters (dict[str, Any]): Combined parameters from both conditions.
    """

    def __init__(
        self,
        left: SQLCondition,
        operator: ESQLLogicalOperator,
        right: SQLCondition,
    ) -> None:
        """Initialize a SQLCompoundCondition instance.

        Args:
            left (SQLCondition): The left-hand side condition.
            operator (ESQLLogicalOperator): The logical operator (e.g., AND, OR).
            right (SQLCondition): The right-hand side condition.
        """
        self.left = left  # type: ignore
        self.operator: ESQLLogicalOperator = operator  # type: ignore
        self.right = right  # type: ignore
        self.parameters = left.parameters | right.parameters

    def to_sql(self) -> str:
        """Convert the compound condition to its SQL representation.

        Returns:
            str: The SQL representation of the compound condition.
        """
        return f"({self.left} {self.operator} {self.right})"

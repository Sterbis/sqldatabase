from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from .sqlcondition import SQLCondition
from .sqloperator import ESQLComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SQLColumn


class SQLColumnFilter(SQLCondition):
    """Represents a filter applied to a SQL column.

    Attributes:
        operator (ESQLComparisonOperator): The comparison operator used in the filter.
        column (SQLColumn): The column to which the filter is applied.
    """

    operator: ESQLComparisonOperator

    def __init__(self, column: SQLColumn, *values) -> None:
        """Initialize a SQLColumnFilter instance.

        Args:
            column (SQLColumn): The column to which the filter is applied.
            *values: The values used in the filter.
        """
        SQLCondition.__init__(self, column, self.operator, *values)
        self.column = column


class SQLValueColumnFilter(SQLColumnFilter):
    """Represents a filter that uses a single value for comparison.

    Attributes:
        value (Any): The value used in the filter.
    """

    def __init__(self, column: SQLColumn, value: Any) -> None:
        """Initialize a ValueColumnFilter instance.

        Args:
            column (SQLColumn): The column to which the filter is applied.
            value (Any): The value used in the filter.
        """
        SQLColumnFilter.__init__(self, column, value)
        self.value = value


class SQLEqualColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks for equality."""

    operator = ESQLComparisonOperator.EQUAL


class SQLNotEqualColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks for inequality."""

    operator = ESQLComparisonOperator.NOT_EQUAL


class SQLLessThanColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value is less than another."""

    operator = ESQLComparisonOperator.LESS_THAN


class SQLLessThanOrEqualColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value is less than or equal to another."""

    operator = ESQLComparisonOperator.LESS_THAN_OR_EQUAL


class SQLGreaterThanColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value is greater than another."""

    operator = ESQLComparisonOperator.GREATER_THAN


class SQLGreaterThanOrEqualColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value is greater than or equal to another."""

    operator = ESQLComparisonOperator.GREATER_THAN_OR_EQUAL


class SQLLikeColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value matches a pattern."""

    operator = ESQLComparisonOperator.LIKE


class SQLNotLikeColumnFilter(SQLValueColumnFilter):
    """Represents a filter that checks if a value does not match a pattern."""

    operator = ESQLComparisonOperator.NOT_LIKE


class SQLValuesColumnFilter(SQLColumnFilter):
    """Represents a filter that uses multiple values for comparison.

    Attributes:
        values (Iterable): The values used in the filter.
    """

    def __init__(self, column: SQLColumn, values: Iterable) -> None:
        """Initialize a ValuesColumnFilter instance.

        Args:
            column (SQLColumn): The column to which the filter is applied.
            values (Iterable): The values used in the filter.
        """
        SQLColumnFilter.__init__(self, column, *values)


class SQLInColumnFilter(SQLValuesColumnFilter):
    """Represents a filter that checks if a value is in a set of values."""

    operator = ESQLComparisonOperator.IN


class SQLNotInColumnFilter(SQLValuesColumnFilter):
    """Represents a filter that checks if a value is not in a set of values."""

    operator = ESQLComparisonOperator.NOT_IN


class SQLBetweenBaseColumnFilter(SQLColumnFilter):
    """Represents a filter that checks if a value is between two bounds.

    Attributes:
        lower_value (Any): The lower bound value.
        upper_value (Any): The upper bound value.
    """

    def __init__(self, column: SQLColumn, lower_value: Any, upper_value: Any):
        """Initialize a BetweenColumnFilter instance.

        Args:
            column (SQLColumn): The column to which the filter is applied.
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.
        """
        SQLColumnFilter.__init__(self, column, lower_value, upper_value)
        self.lower_value = lower_value
        self.upper_value = upper_value


class SQLBetweenColumnFilter(SQLBetweenBaseColumnFilter):
    """Represents a filter that checks if a value is between two bounds."""

    operator = ESQLComparisonOperator.BETWEEN


class SQLNotBetweenColumnFilter(SQLBetweenBaseColumnFilter):
    """Represents a filter that checks if a value is not between two bounds."""

    operator = ESQLComparisonOperator.NOT_BETWEEN


class SQLIsNullBaseColumnFilter(SQLColumnFilter):
    """Represents a filter that checks for null values."""

    def __init__(self, column: SQLColumn):
        SQLColumnFilter.__init__(self, column)


class SQLIsNullColumnFilter(SQLIsNullBaseColumnFilter):
    """Represents a filter that checks if a value is null."""

    operator = ESQLComparisonOperator.IS_NULL


class SQLIsNotNullColumnFilter(SQLIsNullBaseColumnFilter):
    """Represents a filter that checks if a value is not null."""

    operator = ESQLComparisonOperator.IS_NOT_NULL


class SQLColumnFilters:
    """Represents filters that can be applied to a SQL column.

    Attributes:
        column (SQLColumn): The column to which the filters are applied.
    """

    def __init__(self, column: SQLColumn):
        """Initialize a SQLColumnFilters instance.

        Args:
            column (SQLColumn): The column to which the filters are applied.
        """
        self.column = column

    def BETWEEN(self, lower_value: Any, upper_value: Any) -> SQLBetweenColumnFilter:
        """
        Create an BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            SQLBetweenColumnFilter: The created filter.
        """
        return SQLBetweenColumnFilter(self.column, lower_value, upper_value)

    def EQUAL(self, value: Any) -> SQLEqualColumnFilter:
        """
        Create an EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLEqualColumnFilter: The created filter.
        """
        return SQLEqualColumnFilter(self.column, value)

    def GREATER_THAN(self, value: Any) -> SQLGreaterThanColumnFilter:
        """
        Create an GREATER_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLGreaterThanColumnFilter: The created filter.
        """
        return SQLGreaterThanColumnFilter(self.column, value)

    def GREATER_THAN_OR_EQUAL(self, value: Any) -> SQLGreaterThanOrEqualColumnFilter:
        """
        Create an GREATER_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLGreaterThanOrEqualColumnFilter: The created filter.
        """
        return SQLGreaterThanOrEqualColumnFilter(self.column, value)

    def IN(self, values: Iterable) -> SQLInColumnFilter:
        """
        Create an IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            SQLInColumnFilter: The created filter.
        """
        return SQLInColumnFilter(self.column, values)

    def IS_NOT_NULL(self) -> SQLIsNotNullColumnFilter:
        """
        Create an IS_NOT_NULL filter.

        Returns:
            SQLIsNotNullColumnFilter: The created filter.
        """
        return SQLIsNotNullColumnFilter(self.column)

    def IS_NULL(self) -> SQLIsNullColumnFilter:
        """
        Create an IS_NULL filter.

        Returns:
            SQLIsNullColumnFilter: The created filter.
        """
        return SQLIsNullColumnFilter(self.column)

    def LESS_THAN(self, value: Any) -> SQLLessThanColumnFilter:
        """
        Create an LESS_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLessThanColumnFilter: The created filter.
        """
        return SQLLessThanColumnFilter(self.column, value)

    def LESS_THAN_OR_EQUAL(self, value: Any) -> SQLLessThanOrEqualColumnFilter:
        """
        Create an LESS_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLessThanOrEqualColumnFilter: The created filter.
        """
        return SQLLessThanOrEqualColumnFilter(self.column, value)

    def LIKE(self, value: Any) -> SQLLikeColumnFilter:
        """
        Create an LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLLikeColumnFilter: The created filter.
        """
        return SQLLikeColumnFilter(self.column, value)

    def NOT_BETWEEN(
        self, lower_value: Any, upper_value: Any
    ) -> SQLNotBetweenColumnFilter:
        """
        Create an NOT_BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            SQLNotBetweenColumnFilter: The created filter.
        """
        return SQLNotBetweenColumnFilter(self.column, lower_value, upper_value)

    def NOT_EQUAL(self, value: Any) -> SQLNotEqualColumnFilter:
        """
        Create an NOT_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLNotEqualColumnFilter: The created filter.
        """
        return SQLNotEqualColumnFilter(self.column, value)

    def NOT_IN(self, values: Iterable) -> SQLNotInColumnFilter:
        """
        Create an NOT_IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            SQLNotInColumnFilter: The created filter.
        """
        return SQLNotInColumnFilter(self.column, values)

    def NOT_LIKE(self, value: Any) -> SQLNotLikeColumnFilter:
        """
        Create an NOT_LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            SQLNotLikeColumnFilter: The created filter.
        """
        return SQLNotLikeColumnFilter(self.column, value)

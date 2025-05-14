from .sqlbase import SQLBaseEnum


class ESQLComparisonOperator(SQLBaseEnum):
    """Enumeration for SQL comparison operators.

    Attributes:
        BETWEEN (str): Represents the 'BETWEEN' operator.
        EQUAL (str): Represents the '=' operator.
        GREATER_THAN (str): Represents the '>' operator.
        GREATER_THAN_OR_EQUAL (str): Represents the '>=' operator.
        IN (str): Represents the 'IN' operator.
        IS_NOT_NULL (str): Represents the 'IS NOT NULL' operator.
        IS_NULL (str): Represents the 'IS NULL' operator.
        LESS_THAN (str): Represents the '<' operator.
        LESS_THAN_OR_EQUAL (str): Represents the '<=' operator.
        LIKE (str): Represents the 'LIKE' operator.
        NOT_BETWEEN (str): Represents the 'NOT BETWEEN' operator.
        NOT_EQUAL (str): Represents the '!=' operator.
        NOT_IN (str): Represents the 'NOT IN' operator.
        NOT_LIKE (str): Represents the 'NOT LIKE' operator.
    """

    BETWEEN = "BETWEEN"
    EQUAL = "="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = "<="
    IN = "IN"
    IS_NOT_NULL = "IS NOT NULL"
    IS_NULL = "IS NULL"
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    LIKE = "LIKE"
    NOT_BETWEEN = "NOT BETWEEN"
    NOT_EQUAL = "!="
    NOT_IN = "NOT IN"
    NOT_LIKE = "NOT LIKE"


class ESQLLogicalOperator(SQLBaseEnum):
    """Enumeration for SQL logical operators.

    Attributes:
        AND (str): Represents the 'AND' operator.
        OR (str): Represents the 'OR' operator.
    """

    AND = "AND"
    OR = "OR"

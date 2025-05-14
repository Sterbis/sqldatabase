from sqldatabase import (
    SQLColumn,
    SQLColumnsWithID,
    SQLDataTypes,
    SQLiteDatabase,
    SQLServerDatabase,
    SQLTable,
    SQLTables,
)


class UsersTableColumns(SQLColumnsWithID):
    TIMESTAMP = SQLColumn("timestamp", SQLDataTypes.FLOAT, default_value=0.1)
    USERNAME = SQLColumn("username", SQLDataTypes.TEXT, not_null=True, unique=True)
    EMAIL = SQLColumn("email", SQLDataTypes.TEXT, not_null=True, unique=True)
    PHOTO = SQLColumn("photo", SQLDataTypes.BLOB)
    ADMIN = SQLColumn("admin", SQLDataTypes.BOOLEAN, not_null=True)
    BIRTH_DATE = SQLColumn("birth_date", SQLDataTypes.DATE)
    LAST_LOGIN = SQLColumn("last_login", SQLDataTypes.DATETIME)
    AUTOMATIC_LOGOUT = SQLColumn("automatic_logout", SQLDataTypes.TIME)


class UsersTable(SQLTable[UsersTableColumns]):
    name = "users"
    columns = UsersTableColumns()


class UsersDatabaseTables(SQLTables):
    USERS = UsersTable()


class UsersSQLiteDatabase(SQLiteDatabase[UsersDatabaseTables]):
    tables = UsersDatabaseTables()


class UsersSQLServerDatabase(SQLServerDatabase[UsersDatabaseTables]):
    tables = UsersDatabaseTables()

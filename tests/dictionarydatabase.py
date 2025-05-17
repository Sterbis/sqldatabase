from enum import Enum

from sqldatabase import (
    ESQLForeignKeyAction,
    SQLColumn,
    SQLColumns,
    SQLColumnsWithID,
    SQLDataTypes,
    SQLiteDatabase,
    SQLServerDatabase,
    SQLTable,
    SQLTables,
)


class EPartOfSpeech(Enum):
    NOUN = "noun"
    VERB = "verb"
    ADJECTIVE = "adjective"
    ADVERB = "adverb"
    PRONOUN = "pronoun"
    PREPOSITION = "preposition"
    CONJUNCTION = "conjunction"
    INTERJECTION = "interjection"
    DETERMINER = "determiner"


class ETag(Enum):
    ACTION = "action"
    BUSINESS = "business"
    EVERYDAY = "everyday"
    SCIENCE = "science"
    WEIGHT = "weight"


class WordsTableColumns(SQLColumnsWithID):
    WORD = SQLColumn("word", SQLDataTypes.TEXT, not_null=True, unique=True)
    PRONUNCIATION = SQLColumn("pronunciation", SQLDataTypes.TEXT)


class WordsTable(SQLTable[WordsTableColumns]):
    name = "words"
    columns = WordsTableColumns()


class MeaningsTableColumns(SQLColumnsWithID):
    WORD_ID = SQLColumn(
        "word_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=WordsTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )
    DEFINITION = SQLColumn("definition", SQLDataTypes.TEXT, not_null=True)
    PART_OF_SPEECH = SQLColumn(
        "part_of_speech", SQLDataTypes.TEXT, values=EPartOfSpeech
    )


class MeaningsTable(SQLTable[MeaningsTableColumns]):
    name = "meanings"
    columns = MeaningsTableColumns()


class ExamplesTableColumns(SQLColumnsWithID):
    MEANING_ID = SQLColumn(
        "meaning_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )
    EXAMPLE = SQLColumn("example", SQLDataTypes.TEXT)


class ExamplesTable(SQLTable[ExamplesTableColumns]):
    name = "examples"
    columns = ExamplesTableColumns()


class TagsTableColumns(SQLColumnsWithID):
    TAG = SQLColumn("tag", SQLDataTypes.TEXT, not_null=True, unique=True, values=ETag)


class TagsTable(SQLTable[TagsTableColumns]):
    name = "tags"
    columns = TagsTableColumns()


class MeaningTagsTableColumns(SQLColumns):
    MEANING_ID = SQLColumn(
        "meaning_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )
    TAG_ID = SQLColumn(
        "tag_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=TagsTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )


class MeaningTagsTable(SQLTable[MeaningTagsTableColumns]):
    name = "meaning_tags"
    columns = MeaningTagsTableColumns()


class UsersTableColumns(SQLColumnsWithID):
    USERNAME = SQLColumn("username", SQLDataTypes.TEXT, not_null=True, unique=True)
    EMAIL = SQLColumn("email", SQLDataTypes.TEXT, not_null=True, unique=True)


class UsersTable(SQLTable[UsersTableColumns]):
    name = "users"
    columns = UsersTableColumns()


class UserProgressTableColumns(SQLColumns):
    USER_ID = SQLColumn(
        "user_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=UsersTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )
    MEANING_ID = SQLColumn(
        "meaning_id",
        SQLDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
        on_delete=ESQLForeignKeyAction.CASCADE,
    )
    ATTEMPTS = SQLColumn("attempts", SQLDataTypes.INTEGER, default_value=0)
    CORRECT = SQLColumn("correct", SQLDataTypes.INTEGER, default_value=0)
    LAST_SEEN = SQLColumn("last_seen", SQLDataTypes.DATE)


class UserProgressTable(SQLTable[UserProgressTableColumns]):
    name = "user_progress"
    columns = UserProgressTableColumns()


class DictionaryDatabaseTables(SQLTables):
    WORDS = WordsTable()
    MEANIGS = MeaningsTable()
    EXAMPLES = ExamplesTable()
    TAGS = TagsTable()
    MEANING_TAGS = MeaningTagsTable()
    USERS = UsersTable()
    USER_PROGRESS = UserProgressTable()


class DictionarySQLiteDatabase(SQLiteDatabase[DictionaryDatabaseTables]):
    tables = DictionaryDatabaseTables()


class DictionarySQLServerDatabase(SQLServerDatabase[DictionaryDatabaseTables]):
    tables = DictionaryDatabaseTables()

# ------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2018 Luzzi Valerio
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        sqlitedb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     31/07/2018
# ------------------------------------------------------------------------------
import os,sys,re
import sqlite3 as sqlite


class SqliteDB(AbstractDB):
    """
    SqliteDB -
    """
    CORE_FUNCTIONS = ["ABS", "CHANGES", "CHAR", "COALESCE", "GLOB", "HEX", "IFNULL", "INSTR", "LAST_INSERT_ROWID",
                      "LENGTH", "LIKE", "LIKE", "LIKELIHOOD", "LIKELY", "LOAD_EXTENSION", "LOAD_EXTENSION", "LOWER",
                      "LTRIM", "LTRIM", "MAX", "MIN", "NULLIF", "PRINTF", "QUOTE", "RANDOM", "RANDOMBLOB", "REPLACE",
                      "ROUND", "ROUND", "RTRIM", "RTRIM", "SOUNDEX", "SQLITE_COMPILEOPTION_GET",
                      "SQLITE_COMPILEOPTION_USED", "SQLITE_SOURCE_ID", "SQLITE_VERSION", "SUBSTR", "SUBSTR",
                      "TOTAL_CHANGES", "TRIM", "TRIM", "TYPEOF", "UNICODE", "UNLIKELY", "UPPER", "ZEROBLOB"]
    AGGREGATE_FUNCTIONS = ["AVG", "COUNT", "GROUP_CONCAT", "MAX", "MIN", "SUM", "TOTAL"]
    DATE_FUNCTIONS = ["DATE", "TIME", "DATETIME", "JULIANDAY", "STRFTIME"]
    SPATIAL_FUNCTIONS = ["GEOMFROMTEXT", "GEOMFROMWKB", "ASTEXT", "POINT", "X", "Y"]
    RESERVED_WORDS = ["ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ATTACH",
                      "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK",
                      "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
                      "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE",
                      "DESC", "DETACH", "DISTINCT", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE",
                      "EXISTS", "EXPLAIN", "FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", "GROUP", "HAVING", "IF",
                      "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD",
                      "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL",
                      "NO", "NOT", "NOTNULL", "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PLAN", "PRAGMA",
                      "PRIMARY", "QUERY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME",
                      "REPLACE", "RESTRICT", "RIGHT", "ROLLBACK", "ROW", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP",
                      "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER", "UNION", "UNIQUE", "UPDATE", "USING",
                      "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WITH", "WITHOUT"]

    SQLITE_FUNCTIONS = CORE_FUNCTIONS + AGGREGATE_FUNCTIONS + DATE_FUNCTIONS + SPATIAL_FUNCTIONS + RESERVED_WORDS

    def __init__(self, filename, modules=[]):
        """
        Constructor
        :param filename:
        """
        AbstractDB.__init__(self, filename)
        self.pragma("synchronous=OFF")
        self.pragma("journal_mode=WAL")
        self.pragma("foreign_keys=ON")
        self.pragma("cache_size=4000")
##        self.load_extension(modules)
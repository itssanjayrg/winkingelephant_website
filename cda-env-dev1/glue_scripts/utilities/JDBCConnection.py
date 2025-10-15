from typing import Protocol


# noinspection PyPep8Naming
class JDBCResultSet(Protocol):
    def next(self) -> None:
        """Next"""

    def getInt(self, col_name: str) -> int:
        """Get Int"""


# noinspection PyPep8Naming
class JDBCStatement(Protocol):
    def executeUpdate(self, query: str) -> int:
        """Execute update"""

    def executeQuery(self, query: str) -> JDBCResultSet:
        """Execute query"""

    def execute(self, query: str) -> None:
        """Execute query"""

    def close(self) -> None:
        """Close"""


# noinspection PyPep8Naming
class JDBCConnection(Protocol):
    def createStatement(self) -> JDBCStatement:
        """Create statement"""

    def close(self) -> None:
        """Close JDBC connection"""

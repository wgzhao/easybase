"""
EasyBase, a developer-friendly Python library to interact with Apache
HBase.
"""

from ._version import __version__

from .connection import DEFAULT_HOST, DEFAULT_PORT, Connection
from .table import Table
from .pool import ConnectionPool, NoConnectionsAvailable

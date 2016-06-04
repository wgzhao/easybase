"""
EasyBase, a developer-friendly Python library to interact with Apache
HBase.
"""

from ._version import __version__

import thriftpy as _thriftpy
_thriftpy

from .connection import DEFAULT_HOST, DEFAULT_PORT, Connection
from .table import Table
from .batch import Batch
from .pool import ConnectionPool, NoConnectionsAvailable

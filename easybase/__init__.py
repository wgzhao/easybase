"""
EasyBase, a developer-friendly Python library to interact with Apache
HBase. Support Time-Range scan and multi-version access
"""
# using thriftpy2 server as thrift bridge directly
import importlib.resources as resources
import thriftpy2


def _get_thrift_path():
    """
    Locate the packaged HBase.thrift file in a Python 3-only way.

    Python 3.9+ provides resources.files; for 3.7/3.8 we fall back to the
    context manager `resources.path`.
    """
    try:
        return resources.files(__name__.rsplit('.', 1)[0]).joinpath("HBase.thrift")
    except AttributeError:
        # Python 3.7/3.8
        with resources.path(__name__.rsplit('.', 1)[0], "HBase.thrift") as p:
            return p


thriftpy2.load(
    str(_get_thrift_path()),
    module_name='HBase_thrift'
)

from .connection import DEFAULT_HOST, DEFAULT_PORT, Connection
from .table import Table
from .pool import ConnectionPool, NoConnectionsAvailable

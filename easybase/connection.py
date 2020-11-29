# coding: UTF-8

"""
EasyBase connection module.
"""

import logging
from six import iteritems, binary_type, text_type

from thriftpy2.thrift import TApplicationException

from thriftpy2.transport import TBufferedTransport, TFramedTransport
from thriftpy2.protocol import TBinaryProtocol, TCompactProtocol
from thriftpy2.rpc import make_client

from HBase_thrift import TTableName, TTimeRange, TColumnFamilyDescriptor, TTableDescriptor, TNamespaceDescriptor
from HBase_thrift import THBaseService as HBase
from HBase_thrift import TIOError

from .kerberos import TSaslClientTransport
from .table import Table
from .util import pep8_to_camel_case

logger = logging.getLogger(__name__)

COMPAT_MODES = ('0.90', '0.92', '0.94', '0.96', '0.98', '2.2.0')
THRIFT_TRANSPORTS = dict(
    buffered=TBufferedTransport,
    framed=TFramedTransport,
)
THRIFT_PROTOCOLS = dict(
    binary=TBinaryProtocol,
    compact=TCompactProtocol,
)

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.96'
DEFAULT_PROTOCOL = 'binary'

STRING_OR_BINARY = (binary_type, text_type)


class Connection(object):
    """Connection to an HBase Thrift server.

    The `host` and `port` arguments specify the host name and TCP port
    of the HBase Thrift server to connect to. If omitted or ``None``,
    a connection to the default port on ``localhost`` is made. If
    specifed, the `timeout` argument specifies the socket timeout in
    milliseconds.

    If `autoconnect` is `True` (the default) the connection is made
    directly, otherwise :py:meth:`Connection.open` must be called
    explicitly before first use.

    The optional `table_prefix` and `table_prefix_separator` arguments
    specify a prefix and a separator string to be prepended to all table
    names, e.g. when :py:meth:`Connection.table` is invoked. For
    example, if `table_prefix` is ``myproject``, all tables tables will
    have names like ``myproject_XYZ``.

    The optional `compat` argument sets the compatibility level for
    this connection. Older HBase versions have slightly different Thrift
    interfaces, and using the wrong protocol can lead to crashes caused
    by communication errors, so make sure to use the correct one. This
    value can be either the string ``0.90``, ``0.92``, ``0.94``, or
    ``0.96`` (the default).

    The optional `transport` argument specifies the Thrift transport
    mode to use. Supported values for this argument are ``buffered``
    (the default) and ``framed``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. HBase versions before 0.94
    always use the buffered transport. Starting with HBase 0.94, the
    Thrift server optionally uses a framed transport, depending on the
    argument passed to the ``hbase-daemon.sh start thrift`` command.
    The default ``-threadpool`` mode uses the buffered transport; the
    ``-hsha``, ``-nonblocking``, and ``-threadedselector`` modes use the
    framed transport.

    The optional `protocol` argument specifies the Thrift transport
    protocol to use. Supported values for this argument are ``binary``
    (the default) and ``compact``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. ``TCompactProtocol`` is
    a more compact binary format that is  typically more efficient to
    process as well. ``TBinaryProtocol`` is the default protocol that
    Happybase uses.

    .. versionadded:: 0.9
       `protocol` argument

    .. versionadded:: 0.5
       `timeout` argument

    .. versionadded:: 0.4
       `table_prefix_separator` argument

    .. versionadded:: 0.4
       support for framed Thrift transports

    :param str host: The host to connect to
    :param int port: The port to connect to
    :param int timeout: The socket timeout in milliseconds (optional)
    :param bool autoconnect: Whether the connection should be opened directly
    :param str table_prefix: Prefix used to construct table names (optional)
    :param str table_prefix_separator: Separator used for `table_prefix`
    :param str compat: Compatibility mode (optional)
    :param str transport: Thrift transport mode (optional)
    :param bool use_kerberos: Whether enable kerberos support or not (optional)
    :param str sasl_service_name: The HBase's kerberos service name, defaults to 'hbase' (optional)
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None,
                 autoconnect=True, table_prefix=None,
                 table_prefix_separator='_', compat=DEFAULT_COMPAT,
                 transport=DEFAULT_TRANSPORT, protocol=DEFAULT_PROTOCOL,
                 use_kerberos=False, sasl_service_name='hbase'):
        # type: (str, int, int, bool, str, str, str, str, str, bool, str) -> None
        if transport not in THRIFT_TRANSPORTS:
            raise ValueError("'transport' must be one of %s"
                             % ", ".join(THRIFT_TRANSPORTS.keys()))

        if table_prefix is not None \
                and not isinstance(table_prefix, str):
            raise TypeError("'table_prefix' must be a string")

        if not isinstance(table_prefix_separator, str):
            raise TypeError("'table_prefix_separator' must be a string")

        if compat not in COMPAT_MODES:
            raise ValueError("'compat' must be one of %s"
                             % ", ".join(COMPAT_MODES))

        if protocol not in THRIFT_PROTOCOLS:
            raise ValueError("'protocol' must be one of %s"
                             % ", ".join(THRIFT_PROTOCOLS))

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat
        self.use_kerberos = use_kerberos
        self.sasl_service_name = sasl_service_name

        self._transport_class = THRIFT_TRANSPORTS[transport]
        self._protocol_class = THRIFT_PROTOCOLS[protocol]
        self._refresh_thrift_client()

        if autoconnect:
            self.open()

        self._initialized = True

    def _refresh_thrift_client(self):
        # socket = TSocket(host=self.host, port=self.port, socket_timeout=self.timeout)

        # self.transport = self._transport_class()
        # protocol = self._protocol_class(self.transport, decode_response=False)
        """Refresh the Thrift socket, transport, and client."""
        if self.use_kerberos:
            transport = TSaslClientTransport(self._transport_class, self.host, self.sasl_service_name)
            self.client = make_client(HBase, self.host, port=self.port,
                                      # proto_factory=protocol,
                                      trans_factory=transport,
                                      timeout=self.timeout)
        else:
            self.client = make_client(HBase, self.host, port=self.port, timeout=self.timeout)

    def _table_name(self, name):
        # type: (str) -> str
        """Construct a table name by optionally adding a table name prefix."""
        if self.table_prefix is None:
            return name

        return self.table_prefix + self.table_prefix_separator + name

    def open(self):
        """Open the underlying transport to the HBase instance.

        This method opens the underlying Thrift transport (TCP connection).
        """
        # if self.transport.is_open():
        #    return

        logger.debug("Opening Thrift transport to %s:%d", self.host, self.port)
        # self.transport.open()

    def close(self):
        """Close the underyling transport to the HBase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        self.client.close()
        # if not self.transport.is_open():
        #     return

        # if logger is not None:
        #     # If called from __del__(), module variables may no longer
        #     # exist.
        #     logger.debug(
        #         "Closing Thrift transport to %s:%d",
        #         self.host, self.port)

        # self.transport.close()

    def table(self, name, use_prefix=True):
        # type: (str, bool) -> Table
        """Return a table object.

        Returns a :py:class:`easybase.Table` instance for the table
        named `name`. This does not result in a round-trip to the
        server, and the table is not checked for existence.

        The optional `use_prefix` argument specifies whether the table
        prefix (if any) is prepended to the specified `name`. Set this
        to `False` if you want to use a table that resides in another
        ‘prefix namespace’, e.g. a table from a ‘friendly’ application
        co-hosted on the same HBase instance. See the `table_prefix`
        argument to the :py:class:`Connection` constructor for more
        information.

        :param str name: the name of the table
        :param bool use_prefix: whether to use the table prefix (if any)
        :return: Table instance
        :rtype: :py:class:`Table`
        """
        if use_prefix:
            name = self._table_name(name)
        return Table(name, self)

    #
    # Table administration and maintenance
    #

    def tables(self):
        """Return a list of table names available in this HBase instance.

        If a `table_prefix` was set for this :py:class:`Connection`, only
        tables that have the specified prefix will be listed.

        :return: The table names
        :rtype: List of strings
        """
        if self.table_prefix is not None:
            tbl_pattern = self.table_prefix + '.*'
        else:
            tbl_pattern = '.*'
        names = self.search_table(tbl_pattern, include_systable=False)
        if names:
            offset = len(self.table_prefix) if self.table_prefix else 0
            names = [n[offset:] for n in names]

        return names

    def create_table(self, name, families, ns_name=None):
        # type: (str, dict, str) -> None
        """Create a table.

        :param str name: The table name
        :param dict families: The name and options for each column family
        :param str ns_name: the name of namespace, defaults to None

        The `families` argument is a dictionary mapping column family
        names to a dictionary containing the options for this column
        family, e.g.

        ::

            families = {
                'cf1': dict(max_versions=10),
                'cf2': dict(max_versions=1, block_cache_enabled=False),
                'cf3': dict(),  # use defaults
            }
            connection.create_table('mytable', families)

        These options correspond to the ColumnDescriptor structure in
        the Thrift API, but note that the names should be provided in
        Python style, not in camel case notation, e.g. `time_to_live`,
        not `timeToLive`. The following options are supported:

        * ``max_versions`` (`int`)
        * ``compression`` (`str`)
        * ``in_memory`` (`bool`)
        * ``bloom_filter_type`` (`str`)
        * ``bloom_filter_vector_size`` (`int`)
        * ``bloom_filter_nb_hashes`` (`int`)
        * ``block_cache_enabled`` (`bool`)
        * ``time_to_live`` (`int`)
        """
        name = self._table_name(name)
        if not isinstance(families, dict):
            raise TypeError("'families' arg must be a dictionary")

        if not families:
            raise ValueError(
                "Cannot create table %r (no column families specified)"
                % name)

        # table_descriptors = [{'tableName': name.encode()}]
        family_desc = []
        for cf_name, options in iteritems(families):
            if options is None:
                options = dict()

            kwargs = dict()
            for option_name, value in iteritems(options):
                if isinstance(value, STRING_OR_BINARY):
                    value = value.encode()

                kwargs[pep8_to_camel_case(option_name)] = value

            # if not cf_name.endswith(':'):
            #     cf_name += ':'
            # kwargs['name'] = cf_name.encode()
            # table_descriptors.append(TTableDescriptor(**kwargs))
            cf = TColumnFamilyDescriptor(name=cf_name.encode(), **kwargs)
            family_desc.append(cf)
        if ns_name and not self.get_namespace(ns_name):
            try:
                self.create_namespace(ns_name)
            except TIOError:
                print("Failed to create namespace: {}".format(ns_name))
                return

        tbl_name = TTableName(ns=ns_name, qualifier=name.encode())
        tdesc = TTableDescriptor(tableName=tbl_name, columns=family_desc)
        try:
            self.client.createTable(tdesc, splitKeys=None)
        except TApplicationException:
            raise NotImplementedError("current thrift not support create_table method")
        except TIOError as e:
            print(e.message)

    def delete_table(self, name, disable=False, ns_name=None):
        # type: (str, bool, str) -> None
        """Delete the specified table.

        .. versionadded:: 0.5
           `disable` argument

        In HBase, a table always needs to be disabled before it can be
        deleted. If the `disable` argument is `True`, this method first
        disables the table if it wasn't already and then deletes it.

        :param str name: The table name
        :param bool disable: Whether to first disable the table if needed
        :param str ns_name: the namespace name, defaults to none
        """
        if disable and self.is_table_enabled(name):
            self.disable_table(name, ns_name)

        self.client.deleteTable(self.get_tablename(name, ns_name))

    def enable_table(self, name, ns_name=None):
        # type: (str, str) -> None
        """Enable the specified table.

        :param str name: The table name
        :param str ns_name: The tablespace name
        """
        # name = self._table_name(name)
        self.client.enableTable(self.get_tablename(name, ns_name))

    def disable_table(self, name, ns_name=None):
        # type: (str, str) -> None
        """Disable the specified table.

        :param str name: The table name
        :param str ns_name: The namespace name
        """
        # name = self._table_name(name).encode()
        self.client.disableTable(self.get_tablename(name, ns_name))

    def is_table_enabled(self, name, ns_name=None):
        # type: (str, str) -> bool
        """Return whether the specified table is enabled.

        :param str name: The table name
        :param str ns_name: The tablespace name

        :return: whether the table is enabled
        :rtype: bool
        """
        # name = self._table_name(name).encode()
        return self.client.isTableEnabled(self.get_tablename(name, ns_name))

    def compact_table(self, name, major=False):
        # type: (str, bool) -> None
        """Compact the specified table.

        :param str name: The table name
        :param bool major: Whether to perform a major compaction.
        """
        raise NotImplementedError("not implement yet")
        # name = self._table_name(name)
        # if major:
        #     self.client.majorCompact(name)
        # else:
        #     self.client.compact(name)

    def exist_table(self, name, ns_name=None):
        # type: (str, str) -> bool
        """Return whether the sepcified table is exists
        Notes: HBase 1.x not support this method

        :param str name: The table name
        :param str ns_name: The tablespace name
        :return whether the table is exists
        :rtype: bool
        """
        try:
            return self.client.tableExists(self.get_tablename(name, ns_name))
        except TIOError as e:
            return False

    def search_table(self, pattern, include_systable: False):
        # type (str, bool) -> List[String]
        """Return table names of tables that match the given pattern

        :param str pattern:  The regular expression to match against
        :param bool include_systable: set to false if match only against userspace tables
        :return the table names of the matching table
        """
        try:
            result = self.client.getTableNamesByPattern(pattern, include_systable)
            return [x.qualifier for x in result]
        except TIOError:
            return []

    def get_tables_by_namespace(self, ns_name):
        # type(str) -> List[Str]
        """Return names of tables in the given namespace

        :param str ns_name: the namespace's name
        :return the table names in the namespace
        """
        try:
            if self.get_namespace(ns_name):
                result = self.client.getTableNamesByNamespace(ns_name)
                return [x.qualifier for x in result]
        except TIOError as e:
            print(e)
            return []

    def get_tablename(self, name, ns_name=None):
        # type: (str, str) -> TTableName
        """Return the py:class:TTableName class of the spcified table name

        :param str name: The table name
        :param str ns_name: the namespace's name
        :return the py:class:TTableName Class
        :rtype: class
        """
        return TTableName(ns=ns_name, qualifier=self._table_name(name).encode())

    def create_namespace(self, ns_name):
        # type: (str) -> None
        """Create namespace with ns_name

        :param str ns_name: the name of namespace
        """
        tns = TNamespaceDescriptor(ns_name, {})
        try:
            if not self.get_namespace(ns_name):
                self.client.createNamespace(tns)
        except TIOError as e:
            print(e)

    def get_namespace(self, ns_name):
        # type: (str) -> TNamespaceDescriptor
        """Return the py:class:TNamespaceDescriptor class of the specified namepspace name

        :param str ns_name: the namespace name
        :return the py:class:TNamespaceDescriptor Class
        :rtype: class
        """
        try:
            result = self.client.getNamespaceDescriptor(ns_name)
            return result
        except TIOError:
            return None

    def delete_namespace(self, ns_name, cascade=False):
        # type: (str, bool) -> None
        """Delete specified namespace

        :param str ns_name: the namespace name
        :param bool cascade: get ride of namespace with all tables in it
        :return None
        """
        try:
            if self.get_namespace(ns_name):
                # exists table ?
                tbls = self.get_tables_by_namespace(ns_name)
                if tbls:
                    if not cascade:
                        print("namespace {} has {} tables, you cannot drop it without cascade=True option".format(
                                ns_name, len(tbls)
                        ))
                        return
                    else:
                        # drop tables
                        for tbl in tbls:
                            self.delete_table(tbl, disable=True, ns_name=ns_name)
                self.client.deleteNamespace(ns_name)
        except TIOError as e:
            print(e.message)

    def list_namespaces(self):
        """Return a list of namespaces names available in this HBase instance

        :return: The namespace names
        :rtype: List of strings
        """
        try:
            result = self.client.listNamespaceDescriptors()
            return [x.name for x in result]
        except TIOError as e:
            print(e)
            return []
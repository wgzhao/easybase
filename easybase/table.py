"""
EasyBase table module.
"""
import time
import logging
from six import iteritems
from operator import attrgetter
from struct import Struct

from HBase_thrift import TScan, TGet, TColumnValue, TPut, TColumn, TTimeRange, TDelete, TTableName

from .util import str_increment, OrderedDict

logger = logging.getLogger(__name__)

make_cell = attrgetter('value')
make_cell_timestamp = attrgetter('value', 'timestamp')
pack_i64 = Struct('>q').pack


def make_timerange(ts):
    """Make ttypes.TTimeRange format for ts.
    :param list ts: list include at least one timestamp element
    :return TTimeRange or None if ts is None
    """
    if ts is None:
        return ts
    if not isinstance(ts, (tuple, list)):
        raise TypeError("'timerange' must be  list or tuple")
    if len(ts) == 1:
        # only one element, take it as  min timestamp
        ts[1] = int(time.time())

    return TTimeRange(minStamp=ts[0], maxStamp=ts[1])


def make_columns(cols):
    """Make a ttypes.TColumn format for cols
    :param list_or_tuple cols: list of columns ,former of ['cf1:c2','cf2:c2',....]
    :return list: list of TColumns or None if cols is None
    """
    if cols is None:
        return cols
    if not isinstance(cols, (tuple, list)):
        raise TypeError("'columns' must be  list or tuple")
    columns = []
    for c in cols:
        c = c.split(':')
        f, q = (c[0].encode(), c[1].encode()) if len(c) == 2 else (c[0].encode(), None)
        columns.append(TColumn(family=f, qualifier=q))

    return columns


def make_columnvalue(data):
    """Make a ttypes.TColumnValues format for data
    :param dict data: a dict of columns value ,e.g {'cf2:c3': 'v3', 'cf1:c1': 'v1', 'cf1:c2': 'v2'}
    :return list of TColumnValue
    """
    cols = []
    for column, value in iteritems(data):
        f, q = column.split(":")
        cols.append(TColumnValue(family=f.encode(), qualifier=q.encode(), value=value))
    return cols


def make_row(cell_map, include_timestamp):
    """Make a row dict for a cell mapping like ttypes.TRowResult.columns.
    [TColumnValue(family='cf1', qualifier='c1', value='v2', timestamp=456, tags=None, type=4), 
    TColumnValue(family='cf1', qualifier='c2', value='v3', timestamp=1568097958364, tags=None, type=4), 
    TColumnValue(family='cf2', qualifier='c1', value='v4', timestamp=1568097958364, tags=None, type=4), 
    TColumnValue(family='cf2', qualifier='c2', value='v5', timestamp=789, tags=None, type=4)]

    if specify include_timestamp, then the result  of result like below:
        {'cf1:c1':[('v2',456),],
         'cf1:c2': [('v3', 1568097958364),],
         'cf2:c1': [('v4', 1568097958364),],
         'cf2:c2': [('v5', 789),],
         }
    else will return the below:
        {'cf1:c1':'v2',
         'cf1:c2': 'v3',
         'cf2:c1': 'v4',
         'cf2:c2': 'v5',
         }

    """
    rs = {}
    for r in cell_map:
        q = f"{r.family.decode()}:{r.qualifier.decode()}"
        if include_timestamp:
            cell = rs.get(q, [])
            cell.append((r.value, r.timestamp))
            rs[q] = cell
        else:
            rs[q] = r.value
    return rs
    # cellfn = include_timestamp and make_cell_timestamp or make_cell
    # return dict((cn, cellfn(cell)) for cn, cell in cell_map.iteritems())


def make_ordered_row(sorted_columns, include_timestamp):
    """Make a row dict for sorted column results from scans."""
    cellfn = include_timestamp and make_cell_timestamp or make_cell
    return OrderedDict(
        (column.columnName, cellfn(column.cell))
        for column in sorted_columns)


class Table(object):
    """HBase table abstraction class.

    This class cannot be instantiated directly; use :py:meth:`Connection.table`
    instead.
    """

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def __repr__(self):
        return '<%s.%s name=%r>' % (
            __name__,
            self.__class__.__name__,
            self.name,
        )

    def families(self):
        """Retrieve the column families for this table.

        :return: Mapping from column family name to settings dict
        :rtype: dict
        """
        descriptor = self.connection.client.getTableDescriptor(self.get_tablename())

        # convert bytes to string
        families = {cf.name: cf.attributes for cf in descriptor.columns}

        return families

    def _column_family_names(self):
        """Retrieve the column family names for this table (internal use)"""
        names = self.connection.client.getColumnDescriptors(self.name).keys()
        return [name.rstrip(':') for name in names]

    #
    # Data retrieval
    #

    def row(self, row, columns=None, timestamp=None, timerange=None, max_versions=1, include_timestamp=False):
        """Retrieve a single row of data.

        This method retrieves the row with the row key specified in the `row`
        argument and returns the columns and values for this row as
        a dictionary.

        The `row` argument is the row key of the row. If the `columns` argument
        is specified, only the values for these columns will be returned
        instead of all available columns. The `columns` argument should be
        a list or tuple containing strings. Each name can be a column family,
        such as `cf1` or `cf1:` (the trailing colon is not required), or
        a column family with a qualifier, such as `cf1:col1`.

        If specified, the `timestamp` argument specifies the maximum version
        that results may have. The `include_timestamp` argument specifies
        whether cells are returned as single values or as `(value, timestamp)`
        tuples.

        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param list_or_tuple timerange: list of timestamp ,ONLY include 2 elements(option)
        :param int max_versions: number of row's version (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: Mapping of columns (both qualifier and family) to values
        :rtype: dict
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")
        if timerange is not None and not isinstance(timerange, (tuple, list)):
            raise TypeError("'timerange' must be a tuple or list")
        cols = make_columns(columns)
        tt = make_timerange(timerange)

        tget = TGet(row=row.encode(), columns=cols, timestamp=timestamp, timeRange=tt, maxVersions=max_versions)
        result = self.connection.client.get(self.name.encode(), tget)
        if not result:
            return {}
        if max_versions > 1:
            include_timestamp = True
        return make_row(result.columnValues, include_timestamp)

    def rows(self, rows, columns=None, timestamp=None, timerange=None, max_versions=1,
             include_timestamp=False):
        """Retrieve multiple rows of data.

        This method retrieves the rows with the row keys specified in the
        `rows` argument, which should be should be a list (or tuple) of row
        keys. The return value is a list of `(row_key, row_dict)` tuples.

        The `columns`, `timestamp` and `include_timestamp` arguments behave
        exactly the same as for :py:meth:`row`.

        :param list rows: list of row keys
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param list_or_tuple timerange: the range of timestamp, ONLY include 2 elements (optional)
        :param int max_versions: number of row's version (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: List of mappings (columns to values)
        :rtype: list of dicts
        """

        if not rows:
            # Avoid round-trip if the result is empty anyway
            return {}

        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")
        if timerange is not None and not isinstance(timerange, (tuple, list)):
            raise TypeError("'timerange' must be a tuple or list")
        cols = make_columns(columns)
        tt = make_timerange(timerange)

        tgets = []
        for r in rows:
            tgets.append(
                TGet(row=r.encode(), columns=cols, timestamp=timestamp, timeRange=tt, maxVersions=max_versions))
        results = self.connection.client.getMultiple(self.name, tgets)

        return [(r.row, make_row(r.columnValues, include_timestamp))
                for r in results]

    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, filter=None, timerange=None,
             include_timestamp=False, batch_size=1000, scan_batching=None,
             limit=None, reversed=False, max_versions=1):
        """Create a scanner for data in the table.

        This method returns an iterable that can be used for looping over the
        matching rows. Scanners can be created in two ways:

        * The `row_start` and `row_stop` arguments specify the row keys where
          the scanner should start and stop. It does not matter whether the
          table contains any rows with the specified keys: the first row after
          `row_start` will be the first result, and the last row before
          `row_stop` will be the last result. Note that the start of the range
          is inclusive, while the end is exclusive.

          Both `row_start` and `row_stop` can be `None` to specify the start
          and the end of the table respectively. If both are omitted, a full
          table scan is done. Note that this usually results in severe
          performance problems.

        * Alternatively, if `row_prefix` is specified, only rows with row keys
          matching the prefix will be returned. If given, `row_start` and
          `row_stop` cannot be used.

        The `columns`, `timestamp` and `include_timestamp` arguments behave
        exactly the same as for :py:meth:`row`.

        The `filter` argument may be a filter string that will be applied at
        the server by the region servers.

        If `limit` is given, at most `limit` results will be returned.

        The `batch_size` argument specifies how many results should be
        retrieved per batch when retrieving results from the scanner. Only set
        this to a low value (or even 1) if your data is large, since a low
        batch size results in added round-trips to the server.

        The optional `scan_batching` is for advanced usage only; it
        translates to `Scan.setBatching()` at the Java side (inside the
        Thrift server). By setting this value rows may be split into
        partial rows, so result rows may be incomplete, and the number
        of results returned by te scanner may no longer correspond to
        the number of rows matched by the scan.

        If `sorted_columns` is `True`, the columns in the rows returned
        by this scanner will be retrieved in sorted order, and the data
        will be stored in `OrderedDict` instances.

        The optional `max_version` argument specifies how many versions should be
        retrieved per row  

        **Compatibility notes:**

        * The `filter` argument is only available when using HBase 0.92
          (or up). In HBase 0.90 compatibility mode, specifying
          a `filter` raises an exception.

        * The `sorted_columns` argument is only available when using
          HBase 0.96 (or up).

        .. versionadded:: 0.8
           `sorted_columns` argument

        .. versionadded:: 0.8
           `scan_batching` argument

        :param str row_start: the row key to start at (inclusive)
        :param str row_stop: the row key to stop at (exclusive)
        :param str row_prefix: a prefix of the row key that must match
        :param list_or_tuple columns: list of columns (optional)
        :param str filter: a filter string (optional)
        :param list_or_tuple timerange: time range(optional)
        :param bool include_timestamp: whether timestamps are returned
        :param int batch_size: batch size for retrieving resuls
        :param bool scan_batching: server-side scan batching (optional)
        :param int limit: max number of rows to return
        :param bool reversed: whether to reversed
        :param int max_versions: number of row's versions (optional)

        :return: generator yielding the rows matching the scan
        :rtype: iterable of `(row_key, row_data)` tuples
        """
        # convert to bytes
        if row_start:
            row_start = row_start.encode()

        if row_stop:
            row_stop = row_stop.encode()

        if batch_size < 1:
            raise ValueError("'batch_size' must be >= 1")

        if limit is not None and limit < 1:
            raise ValueError("'limit' must be >= 1")

        if scan_batching is not None and scan_batching < 1:
            raise ValueError("'scan_batching' must be >= 1")

        if row_prefix is not None:
            if row_start is not None or row_stop is not None:
                raise TypeError(
                    "'row_prefix' cannot be combined with 'row_start' "
                    "or 'row_stop'")

            row_start = row_prefix.encode()
            row_stop = str_increment(row_prefix).encode()

        # if row_start is None:
        #    row_start = b''

        cols = make_columns(columns)
        tt = make_timerange(timerange)
        if max_versions > 1:
            include_timestamp = True
        tscan = TScan(
            startRow=row_start,
            stopRow=row_stop,
            timeRange=tt,
            columns=cols,
            caching=batch_size,
            filterString=filter,
            batchSize=scan_batching,
            reversed=reversed,
            maxVersions=max_versions,
        )

        scan_id = self.connection.client.openScanner(self.name.encode(), tscan)

        logger.debug("Opened scanner (id=%d) on '%s'", scan_id, self.name)

        n_returned = n_fetched = 0
        try:
            while True:
                if limit is None:
                    how_many = batch_size
                else:
                    how_many = min(batch_size, limit - n_returned)

                items = self.connection.client.getScannerRows(
                    scan_id, how_many)

                if not items:
                    return  # scan has finished

                n_fetched += len(items)

                for n_returned, item in enumerate(items, n_returned + 1):
                    row = make_row(item.columnValues, include_timestamp)

                    yield item.row, row

                    if limit is not None and n_returned == limit:
                        return  # scan has finished
        finally:
            self.connection.client.closeScanner(scan_id)
            logger.debug(
                "Closed scanner (id=%d) on '%s' (%d returned, %d fetched)",
                scan_id, self.name, n_returned, n_fetched)

    #
    # Data manipulation
    #

    def put(self, row, data, timestamp=None, wal=True):
        """Store data in the table.

        This method stores the data in the `data` argument for the row
        specified by `row`. The `data` argument is dictionary that maps columns
        to values. Column names must include a family and qualifier part, e.g.
        `cf:col`, though the qualifier part may be the empty string, e.g.
        `cf:`.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument

        :param str row: the row key
        :param dict data: the data to store
        :param int timestamp: timestamp (optional)
        :param bool wal: whether to write to the WAL (optional)
        """
        # if wal is None:
        #    wal = self.wal
        cols = make_columnvalue(data)

        tput = TPut(row=row.encode(), columnValues=cols, durability=wal, timestamp=timestamp)
        self.connection.client.put(self.name.encode(), tput)

    def puts(self, rows):
        """"Commit a List of Puts to the table

        This method stores the data in sepcified by `row` . the `rows` argument is list that containers multiple `row` .
         e.g
            rows = {
                'r1': {'data':{'cf1:c1':'v1', 'cf2:c2': 'v2'},
                       'wal': True, 'timestamp':123},
                'r2': {'data':{'cf1:c1': 'v2', 'cf2:c2': 'v3'},
                      }
            }
        each `row` is dictionary that the key is row key and the 
        value maps columns to values . Columns names must include a family and qualifier part.

        
        :param dict rows: contains multiple number of `row`
        """
        tputs = []
        for rk, item in iteritems(rows):
            cols = make_columnvalue(item['data'])
            tput = TPut(row=rk.encode(), columnValues=cols,
                        durability=item.get('wal', True),
                        timestamp=item.get('timestamp', None))
            tputs.append(tput)
        self.connection.client.putMultiple(self.name.encode(), tputs)

    def delete(self, row, columns=None, timestamp=None, deletetype=1, attributes=None, durability=False):
        """Delete data from the table.

        This method deletes all columns for the row specified by `row`, or only
        some columns if the `columns` argument is specified.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument
        .. versiondeleted:: 0.9
            `wal` argument
           - row
           - columns
           - timestamp
           - deleteType
           - attributes
           - durability
        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param int deletetype: delete type,default is 1
        :param dict attributes: attributes
        :param int durability:
        """
        cols = make_columns(columns)
        tdelete = TDelete(row=row.encode(), columns=cols, timestamp=timestamp, deleteType=deletetype,
                          attributes=attributes, durability=durability)
        self.connection.client.deleteSingle(self.name.encode(), tdelete)

    #
    # Atomic counters
    #

    def counter_get(self, row, column):
        """Retrieve the current value of a counter column.

        This method retrieves the current value of a counter column. If the
        counter column does not exist, this function initialises it to `0`.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name

        :return: counter value
        :rtype: int
        """
        # Don't query directly, but increment with value=0 so that the counter
        # is correctly initialised if didn't exist yet.
        return self.counter_inc(row, column, value=0)

    def counter_set(self, row, column, value=0):
        """Set a counter column to a specific value.

        This method stores a 64-bit signed integer value in the specified
        column.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name
        :param int value: the counter value to set
        """
        self.put(row, {column: pack_i64(value)})

    def counter_inc(self, row, column, value=1):
        """Atomically increment (or decrements) a counter column.

        This method atomically increments or decrements a counter column in the
        row specified by `row`. The `value` argument specifies how much the
        counter should be incremented (for positive values) or decremented (for
        negative values). If the counter column did not exist, it is
        automatically initialised to 0 before incrementing it.

        :param str row: the row key
        :param str column: the column name
        :param int value: the amount to increment or decrement by (optional)

        :return: counter value after incrementing
        :rtype: int
        """
        return self.connection.client.atomicIncrement(
            self.name, row, column, value)

    def counter_dec(self, row, column, value=1):
        """Atomically decrement (or increments) a counter column.

        This method is a shortcut for calling :py:meth:`Table.counter_inc` with
        the value negated.

        :return: counter value after decrementing
        :rtype: int
        """
        return self.counter_inc(row, column, -value)

    def truncate(self):
        """truncate table

        This method will delete all rows in table

        :return True if successfully else False
        """
        return self.connection.client.truncateTable(self.name, True)

    def get_tablename(self):
        """Return the py:class:TTableName class of the spcified table name

        :return the py:class:TTableName Class
        :rtype: class
        """
        return TTableName(ns=None, qualifier=self.name.encode())

    @staticmethod
    def _bytes2str(obj):
        if isinstance(obj, bytes):
            return obj.decode()
        if isinstance(obj, dict):
            return {x.decode(): y.decode() for x, y in iteritems(obj)}

    def batch(self, timestamp=None, batch_size=None, transaction=False):
        """Create a new batch operation for current table

        This method returns a new :py:class:`Batch` instance that can be 
        used for mass data manipulation. The `timestamp` argument applies 
        all puts and deletes on the batch

        If given, the `batch_size` argument specifies the maximum batch size
        after which the batch should send the mutations to the server, By 
        default this is unbounded.

        The `transaction` argument specifies wether the returned :py:class:`Batch`
        instance should act in a transaction-like manner when used as context manager
        in a ``with`` block of code. The `transaction` flag cannot be used in combination
        with `batch_size`.

        :param int timestamp: timestamp (optional)
        :param int batch_size: batch size (optional)
        :param bool transaction: whether this batch should behave like a transaction

        :return: Batch instance
        :rtype: :py:class:`Batch`
        """
        raise NotImplementedError
        # kwargs = locals().copy()

        # del kwargs['self']
        # return Batch(table=self, **kwargs)

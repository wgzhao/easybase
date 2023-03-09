"""
EasyBase tests.
"""

import os
import random
import threading

from six.moves import range
from six import text_type, iteritems

from thriftpy2.thrift import TDecodeException

from nose.tools import (
    assert_dict_equal,
    assert_equal,
    assert_false,
    assert_not_in,
    assert_in,
    assert_is_instance,
    assert_is_not_none,
    assert_raises,
    assert_true,
    nottest,
)

from easybase.connection import Connection
from easybase.pool import ConnectionPool, NoConnectionsAvailable

EASYBASE_HOST = os.environ.get('EASYBASE_HOST', '127.0.0.1')
EASYBASE_PORT = os.environ.get('EASYBASE_PORT', 9090)
EASYBASE_COMPAT = os.environ.get('EASYBASE_COMPAT', '0.98')
EASYBASE_TRANSPORT = os.environ.get('EASYBASE_TRANSPORT', 'buffered')

TABLE_PREFIX = 'easybase_test_tmp'
TEST_TABLE_NAME = 'test1'

connection_kwargs = dict(zip(
    ('host', 'port', 'table_prefix', 'compat', 'transport'),
    (EASYBASE_HOST,
     EASYBASE_PORT,
     TABLE_PREFIX,
     EASYBASE_COMPAT,
     EASYBASE_TRANSPORT
     ),
))

connection = None
tbl = None


def setup_module():
    global connection, tbl
    connection = Connection(**connection_kwargs)

    assert_is_not_none(connection)

    attempt_delete_table()

    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 10}
    }
    connection.create_table(TEST_TABLE_NAME, families=cfs)

    tbl = connection.table(TEST_TABLE_NAME)
    assert_is_not_none(tbl)


def attempt_delete_table():
    if connection.exist_table(TEST_TABLE_NAME):
        print("Test table already exists; removing it...")
        connection.delete_table(TEST_TABLE_NAME, disable=True)


def test_tablename():
    assert_equal(TABLE_PREFIX + '_' + TEST_TABLE_NAME, tbl.name)


def test_connect_compat():
    with assert_raises(ValueError):
        Connection(compat='a_invalide_version')


def test_timeout_arg():
    Connection(
        host=EASYBASE_HOST,
        port=EASYBASE_PORT,
        timeout=5000,
        autoconnect=False
    )


def test_enabling():
    assert_true(connection.is_table_enabled(TEST_TABLE_NAME))
    connection.disable_table(TEST_TABLE_NAME)
    assert_false(connection.is_table_enabled(TEST_TABLE_NAME))
    connection.enable_table(TEST_TABLE_NAME)
    assert_true(connection.is_table_enabled(TEST_TABLE_NAME))


def test_prefix():
    assert_equal(TABLE_PREFIX + '_', connection._table_name(''))
    assert_equal(TABLE_PREFIX + '_foo', connection._table_name('foo'))

    assert_equal(connection.table('foobar').name, TABLE_PREFIX + '_foobar')
    assert_equal(connection.table('foobar', use_prefix=False).name, 'foobar')

    c = Connection(EASYBASE_HOST, EASYBASE_PORT, autoconnect=False)
    assert_equal('foo', c._table_name('foo'))

    with assert_raises(TypeError):
        Connection(EASYBASE_HOST, EASYBASE_PORT, autoconnect=False, table_prefix=111)

    with assert_raises(TypeError):
        Connection(EASYBASE_HOST, EASYBASE_PORT, autoconnect=False, table_prefix=6.4)


def test_families():
    families = tbl.families()
    for name, fdesc in iteritems(families):
        assert_is_instance(name, text_type)
        assert_is_instance(fdesc, dict)
        # assert_is_instance(fdesc['BLOCKSIZE'], int)
        assert_in('VERSIONS', fdesc)


@nottest
def test_table_region():
    regions = tbl.regions()
    assert_is_instance(regions, list)


def test_invalid_table_create():
    with assert_raises(ValueError):
        connection.create_table('t1', families={})
    with assert_raises(TypeError):
        connection.create_table('t2', families=0)
    with assert_raises(TypeError):
        connection.create_table('t3', families=[])


def test_put():
    tbl.put('r1', {'cf1:c1': 'v1', 'cf2:c2': 'v2'})
    tbl.put('r2', {'cf1:c1': 'v2'}, timestamp=19890604)
    tbl.put('r3', {'cf1:c1': 'v3'}, timestamp=1568028613)

    assert_equal({'cf1:c1': 'v1', 'cf2:c2': 'v2'}, tbl.row('r1'))
    r = tbl.row('r2', include_timestamp=False)
    with assert_raises(IndexError):
        assert_raises(r['cf1:c1'][0][1])
    r = tbl.row('r2', include_timestamp=True)
    assert_equal(19890604, r['cf1:c1'][0][1])

    # cleanup
    tbl.delete('r1')
    tbl.delete('r2')
    tbl.delete('r3')


def test_puts():
    rows = {}
    rks = []
    for i in range(100):
        rk = 'rk_puts_{}'.format(i)
        rks.append(rk)
        rows[rk] = {'data': {'cf1:c1': 'v1', 'cf2:c2': 'v2'}}
        if random.random() > 0.5:
            rows[rk]['wal'] = True
            rows[rk]['timestamp'] = random.randint(100, 1000)
    tbl.puts(rows)

    rs = tbl.rows(rks)
    assert_equal(100, calc_rows(rs))

    # cleanup
    for i in range(100):
        tbl.delete('rk_puts_{}'.format(i))


def test_compaction():
    with assert_raises(NotImplementedError):
        connection.compact_table(TEST_TABLE_NAME)
        connection.compact_table(TEST_TABLE_NAME, major=True)


def test_row():
    row = tbl.row
    put = tbl.put
    rk = 'rk-test'

    with assert_raises(TypeError):
        row(rk, 123)

    with assert_raises(TypeError):
        row(rk, columns='a column string')

    with assert_raises(TypeError):
        row(rk, timerange=123)

    put(rk, {'cf1:c1': 'v1'}, timestamp=123)
    put(rk, {'cf1:c1': 'v2'}, timestamp=456)
    put(rk, {'cf1:c2': 'v3',
             'cf2:c1': 'v4'})
    put(rk, {'cf2:c2': 'v5'}, timestamp=789)

    rs = {
        'cf1:c1': 'v2',
        'cf1:c2': 'v3',
        'cf2:c1': 'v4',
        'cf2:c2': 'v5'
    }

    assert_dict_equal(rs, row(rk, include_timestamp=False))

    rs = {'cf1:c1': [('v2', 456), ],
          'cf2:c2': [('v5', 789), ],
          }

    assert_dict_equal(rs, row(rk, columns=['cf1:c1', 'cf2:c2'], include_timestamp=True))

    rs = {
        'cf1:c1': [('v2', 456), ]
    }
    assert_dict_equal(rs, row(rk, timestamp=456, include_timestamp=True))
    assert_dict_equal({}, row(rk, timestamp=111, include_timestamp=True))

    # cleanup
    tbl.delete(rk)


def test_rows():
    row_keys = ['rk_1', 'rk_2', 'rk_3']
    old_value = {'cf1:c1': 'v_old_c1', 'cf1:c2': 'v_old_c2'}
    new_value = {'cf1:c1': 'v_new_c1', 'cf1:c2': 'v_new_c2'}

    # with assert_raises(TypeError):
    #     tbl.rows(row_keys, object())

    with assert_raises(TDecodeException):
        tbl.rows(row_keys, timestamp='invalid_timestamp')

    for rk in row_keys:
        tbl.put(rk, old_value, timestamp=111)

    for rk in row_keys:
        tbl.put(rk, new_value)

    assert_dict_equal({}, tbl.rows([]))

    rows = dict(tbl.rows(row_keys))

    for rk in row_keys:
        assert_in(rk, rows)
        assert_dict_equal(new_value, rows[rk])

    rows = dict(tbl.rows(row_keys, timestamp=222))
    assert_equal(0, len(rows))

    # cleanup
    for rk in row_keys:
        tbl.delete(rk)


def calc_rows(scanner):
    idx = 0
    for _ in scanner:
        idx += 1
    return idx


def test_scan():
    with assert_raises(TypeError):
        list(tbl.scan(row_prefix='foo', row_start='bar'))

    if connection.compat == '0.99':
        with assert_raises(NotImplementedError):
            list(tbl.scan(filter='foo'))

    with assert_raises(ValueError):
        list(tbl.scan(limit=0))

    with assert_raises(ValueError):
        list(tbl.scan(batch_size=0))

    with assert_raises(ValueError):
        list(tbl.scan(scan_batching=0))

    # write mass rows
    for i in range(1000):
        tbl.put(
            'rk_scan_{:04}'.format(i),
            {
                'cf1:c1': 'v1',
                'cf2:c2': 'v2',
            }
        )

    scanner = tbl.scan(row_start='rk_scan_0010', row_stop='rk_scan_0020', columns=['cf1:c1'])
    assert_equal(10, calc_rows(scanner))

    scanner = tbl.scan(row_start='non_exists', row_stop='end_stop')
    assert_equal(0, calc_rows(scanner))

    scanner = tbl.scan(row_start='rk_scan_', row_stop='rk_scan_0010', columns=['cf2:c2'])

    rk, row = next(scanner)
    assert_equal(rk, 'rk_scan_0000')
    assert_equal(10 - 1, calc_rows(scanner))

    scanner = tbl.scan(row_start='rk_scan_', row_stop='rk_scan_0100', columns=['cf2:c2'], limit=10)
    assert_equal(10, calc_rows(scanner))

    scanner = tbl.scan(row_prefix='rk_scan_01', batch_size=10, limit=20)
    assert_equal(20, calc_rows(scanner))

    scanner = tbl.scan(limit=20)
    next(scanner)
    next(scanner)
    scanner.close()

    with assert_raises(StopIteration):
        next(scanner)

    # cleanup
    for i in range(1000):
        tbl.delete('rk_scan_{:04}'.format(i))


def test_scan_reverse():
    for i in range(1000):
        tbl.put(
            'rk_scan_rev_{:04}'.format(i),
            {
                'cf1:c1': 'v1',
                'cf2:c2': 'v2',
            }
        )

    scanner = tbl.scan(row_start='rk_scan_rev_0999', reversed=True)
    assert_equal(1000, calc_rows(scanner))

    scanner = tbl.scan(limit=10, reversed=True)
    assert_equal(10, calc_rows(scanner))

    scanner = tbl.scan(row_start='rk_scan_rev_0050', row_stop='rk_scan_rev_0000', reversed=True)

    k, _ = next(scanner)
    assert_equal('rk_scan_rev_0050', k)

    assert_equal(50 - 1, calc_rows(scanner))

    # cleanup
    for i in range(1000):
        tbl.delete('rk_scan_rev_{:04}'.format(i))


def test_scan_filter():
    for i in range(10):
        tbl.put(
            'rk_filter_row_{:02}'.format(i),
            {
                'cf1:c1': 'filter_v1',
                'cf2:v2': 'v2'
            }
        )
    _filter = "SingleColumnValueFilter('cf1','c1', = , 'binary:filter_v1')"
    scanner = tbl.scan(filter=_filter)
    assert_equal(10, calc_rows(scanner))

    # cleanup
    for i in range(10):
        tbl.delete('rk_filter_row_{:02}'.format(i))


def test_delete():
    rk = 'rk_test_del'

    cols = {
        'cf1:c1': 'v1',
        'cf1:c2': 'v2',
        'cf2:c1': 'v3',
    }

    tbl.put(rk, {'cf1:c1': 'v1old'}, timestamp=123)
    tbl.put(rk, cols)

    tbl.delete(rk, timestamp=111)
    assert_dict_equal({'cf1:c1': 'v1'}, tbl.row(rk, columns=['cf1:c1']))

    tbl.delete(rk, ['cf1:c1'], timestamp=111)
    assert_equal({}, tbl.row(rk, columns=['cf1:c1'], max_versions=2))

    rs = tbl.row(rk)
    assert_not_in('cf1:c1', rs)
    assert_in('cf1:c2', rs)
    assert_in('cf2:c1', rs)

    tbl.delete(rk)
    assert_dict_equal({}, tbl.row(rk))


def test_connection_pool():
    from thriftpy2.thrift import TException

    def run():
        name = threading.current_thread().name
        print("Thread %s starting" % name)

        def inner_function():
            # Nested connection requests must return the same connection
            with pool.connection() as another_connection:
                assert connection is another_connection

                # Fake an exception once in a while
                if random.random() < .25:
                    print("Introducing random failure")
                    # connection.transport.close()
                    raise TException("Fake transport exception")

        for _ in range(50):
            with pool.connection() as conn:
                conn.table(TEST_TABLE_NAME)

                try:
                    inner_function()
                except TException:
                    # This error should have been picked up by the
                    # connection pool, and the connection should have
                    # been replaced by a fresh one
                    pass

                conn.table(TEST_TABLE_NAME)

        print("Thread %s done" % name)

    N_THREADS = 10

    with assert_raises(TypeError):
        ConnectionPool(EASYBASE_HOST, EASYBASE_PORT, size=[])

    with assert_raises(ValueError):
        ConnectionPool(host=EASYBASE_HOST, port=EASYBASE_PORT, size=0)

    pool = ConnectionPool(host=EASYBASE_HOST, port=EASYBASE_PORT, size=3)
    threads = [threading.Thread(target=run) for i in range(N_THREADS)]

    for t in threads:
        t.start()

    while threads:
        for t in threads:
            t.join(timeout=.1)

        # filter out finished threads
        threads = [t for t in threads if t.is_alive()]
        print("%d threads still alive" % len(threads))


def test_pool_exhaustion():
    pool = ConnectionPool(host=EASYBASE_HOST, port=EASYBASE_PORT, size=1)

    def run():
        with assert_raises(NoConnectionsAvailable):
            with pool.connection(timeout=.1) as connection:
                connection.table(TEST_TABLE_NAME)

    with pool.connection():
        # At this point the only connection is assigned to this thread,
        # so another thread cannot obtain a connection at this point.

        t = threading.Thread(target=run)
        t.start()
        t.join()


if __name__ == '__main__':
    import logging
    import sys

    try:
        import faulthandler
    except ImportError:
        pass
    else:
        import signal

        faulthandler.register(signal.SIGUSR1)

    logging.basicConfig(level=logging.DEBUG)
    setup_module()
    method_name = 'test_{}'.format(sys.argv[1])
    method = globals()[method_name]
    method()

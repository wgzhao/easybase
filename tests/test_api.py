"""
EasyBase tests.
"""

import os
import random
import collections
import threading
import time 


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
    assert_list_equal,
    assert_raises,
    assert_true,
    nottest,
)

from easybase import Connection, ConnectionPool, NoConnectionsAvailable

EASYBASE_HOST = os.environ.get('EASYBASE_HOST', '127.0.0.1')
EASYBASE_PORT = os.environ.get('EASYBASE_PORT', 9090)
EASYBASE_COMPAT = os.environ.get('EASYBASE_COMPAT', '0.96')
EASYBASE_TRANSPORT = os.environ.get('EASYBASE_TRANSPORT', 'buffered')

TABLE_PREFIX = 'easybase_test_tmp'
TEST_TABLE_NAME = 'test1'

connetion_kwargs = dict(zip(
    ('host','port','table_prefix','compat','transport'),
    (EASYBASE_HOST,
    EASYBASE_PORT,
    TABLE_PREFIX,
    EASYBASE_COMPAT,
    EASYBASE_TRANSPORT
    ),
    ))

connection = None
table = None

def setup_module():
    global connection, table
    connection = Connection(**connetion_kwargs)

    assert_is_not_none(connection)

    attempt_delete_table()

    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions':10}
    }
    connection.create_table(TEST_TABLE_NAME, families=cfs)

    table = connection.table(TEST_TABLE_NAME)
    assert_is_not_none(table)

def attempt_delete_table():
    if connection.exist_table(TEST_TABLE_NAME):
        print("Test table already exists; removing it...")
        connection.delete_table(TEST_TABLE_NAME, disable=True)


def test_tablename():
    assert_equal(TABLE_PREFIX + '_' + TEST_TABLE_NAME, table.name)

def test_connect_compat():
    with assert_raises(ValueError):
        Connection(compat='a_invalide_version')

def test_timeout_arg():
    Connection(
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

    c = Connection(autoconnect=False)
    assert_equal('foo', c._table_name('foo'))

    with assert_raises(TypeError):
        Connection(autoconnect=False, table_prefix=111)

    with assert_raises(TypeError):
        Connection(autoconnect=False, table_prefix=6.4)

def test_families():
    families = table.families()
    for name, fdesc in iteritems(families):
        assert_is_instance(name, text_type)
        assert_is_instance(fdesc, dict)
        #assert_is_instance(fdesc['BLOCKSIZE'], int)
        assert_in('VERSIONS', fdesc)

@nottest
def test_table_region():
    regions = table.regions()
    assert_is_instance(regions, list)

def test_invalid_table_create():
    with assert_raises(ValueError):
        connection.create_table('t1', families={})
    with assert_raises(TypeError):
        connection.create_table('t2', families=0)
    with assert_raises(TypeError):
        connection.create_table('t3', families=[])

def test_put():
    table.put('r1', {'cf1:c1': 'v1','cf2:c2': 'v2'})
    table.put('r2', {'cf1:c1': 'v2'}, timestamp=19890604)
    table.put('r3', {'cf1:c1': 'v3'}, timestamp=1568028613)

def test_puts():
    rows = {}
    rks = []
    for i in range(100):
        rk = 'rk_puts_{}'.format(i)
        rks.append(rk)
        rows[rk] = {'data':{'cf1:c1':'v1','cf2:c2':'v2'}}
        if random.random() > 0.5:
            rows[rk]['wal'] = True
            rows[rk]['timestamp'] = random.randint(100,1000)
    table.puts(rows)  
    
    rs = table.rows(rks)
    assert_equal(100, calc_rows(rs))

@nottest
def test_compaction():
    connection.compact_table(TEST_TABLE_NAME)
    connection.compact_table(TEST_TABLE_NAME, major=True)

@nottest
def test_atomic_counters():
    row = b'r-with-counter'
    column = 'cf1:cnt'

    assert_equal(0, table.counter_get(row, column))

    assert_equal(10, table.counter_inc(row, column, 10))
    assert_equal(10, table.counter_get(row, column))

    table.counter_set(row, column, 0)
    assert_equal(1, table.counter_inc(row, column))
    assert_equal(4, table.counter_inc(row, column, 3))
    assert_equal(4, table.counter_get(row, column))

    table.counter_set(row, column, 3)
    assert_equal(3, table.counter_get(row, column))
    assert_equal(8, table.counter_inc(row, column, 5))
    assert_equal(6, table.counter_inc(row, column, -2))
    assert_equal(5, table.counter_dec(row, column))
    assert_equal(3, table.counter_dec(row, column, 2))
    assert_equal(10, table.counter_dec(row, column, -7))

@nottest
def test_batch():
    with assert_raises(TypeError):
        table.batch(timestamp='incorrect')
    b = table.batch()
    b.put(b'row1', {b'cf1:col1': b'value1',
                    b'cf1:col2': b'value2'})
    b.put(b'row2', {b'cf1:col1': b'value1',
                    b'cf1:col2': b'value2',
                    b'cf1:col3': b'value3'})
    b.delete(b'row1', [b'cf1:col4'])
    b.delete(b'another-row')
    b.send()

    b = table.batch(timestamp=1234567)
    b.put(b'r1', {b'cf1:col5': b'value5'})
    b.send()

    with assert_raises(ValueError):
        b = table.batch(batch_size=0)

    with assert_raises(TypeError):
        b = table.batch(transaction=True, batch_size=10)

def test_row():
    row = table.row
    put = table.put
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

    rs =  {'cf1:c1':[('v2',456),],
         'cf2:c2': [('v5', 789),],
         }
    
    assert_dict_equal(rs, row(rk, columns=['cf1:c1','cf2:c2'], include_timestamp=True))

    rs = {
        'cf1:c1': [('v2',456),]
    }
    assert_dict_equal(rs, row(rk, timestamp=456, include_timestamp=True))
    assert_dict_equal({}, row(rk, timestamp=111, include_timestamp=True))

def test_rows():
    row_keys = ['rk_1', 'rk_2','rk_3']
    old_value = {'cf1:c1':'v_old_c1', 'cf1:c2':'v_old_c2'}
    new_value = {'cf1:c1':'v_new_c1', 'cf1:c2':'v_new_c2'}

    # with assert_raises(TypeError):
    #     table.rows(row_keys, object())

    with assert_raises(TDecodeException):
        table.rows(row_keys, timestamp='invalid_timestamp')

    for rk in row_keys:
        table.put(rk, old_value, timestamp=111)
    
    for rk in row_keys:
        table.put(rk, new_value)

    assert_dict_equal({}, table.rows([]))

    rows = dict(table.rows(row_keys))

    for rk in row_keys:
        assert_in(rk, rows)
        assert_dict_equal(new_value, rows[rk])

    rows = dict(table.rows(row_keys, timestamp=222))
    assert_dict_equal({None: {}}, rows)

def calc_rows(scanner):
    return len(list(scanner))

def test_scan():
    with assert_raises(TypeError):
        list(table.scan(row_prefix='foo', row_start='bar'))

    if connection.compat == '0.99':
        with assert_raises(NotImplementedError):
            list(table.scan(filter='foo'))

    with assert_raises(ValueError):
        list(table.scan(limit=0))

    with assert_raises(ValueError):
        list(table.scan(batch_size=0))

    with assert_raises(ValueError):
        list(table.scan(scan_batching=0))

    # write mass rows
    for i in range(1000):
        table.put(
            'rk_scan_{:04}'.format(i),
            {
                'cf1:c1': 'v1',
                'cf2:c2': 'v2',
            }
        )


    scanner = table.scan(row_start='rk_scan_0010', row_stop='rk_scan_0020', columns=['cf1:c1'])
    assert_equal(10, calc_rows(scanner))
    
    scanner = table.scan(row_start='non_exists', row_stop='end_stop')
    assert_equal(0, calc_rows(scanner))

    scanner = table.scan(row_start='rk_scan_', row_stop='rk_scan_0010', columns=['cf2:c2'])

    rk, row = next(scanner)
    assert_equal(rk, 'rk_scan_0000')
    assert_equal(10-1, calc_rows(scanner))

    scanner = table.scan(row_start='rk_scan_', row_stop='rk_scan_0100', columns=['cf2:c2'], limit=10)
    assert_equal(10, calc_rows(scanner))
    
    scanner = table.scan(row_prefix='rk_scan_01', batch_size=10, limit=20)
    assert_equal(20, calc_rows(scanner))

    scanner = table.scan(limit=20)
    next(scanner)
    next(scanner)
    scanner.close()

    with assert_raises(StopIteration):
        next(scanner)

@nottest
def test_scan_reverse():
    for i in range(1000):
        table.put(
            'rk_scan_rev_{:04}'.format(i),
            {
                'cf1:c1': 'v1',
                'cf2:c2': 'v2',
            }
        )

    scanner = table.scan(row_prefix='rk_scan_rev_', reversed=True)
    assert_equal(1000, calc_rows(scanner))

    scanner = table.scan(limit=10, reversed=True)
    assert_equal(10, calc_rows(scanner))

    scanner = table.scan(row_start='rk_scan_rev_0050', row_stop='rk_scan_rev_0000', reversed=True)
    
    k, v = next(scanner)
    assert_equal('rk_scan_rev_0050', k)

    assert_equal(50-1, calc_rows(scanner))

def test_scan_filter():

    _filter = "SingleColumnValueFilter('cf1','c1', = , 'binary:v1')"
    for k, v in table.scan(filter=_filter):
        print(k,v)

def test_delete():
    rk = 'rk_test_del'

    cols = {
        'cf1:c1':'v1',
        'cf1:c2':'v2',
        'cf2:c1':'v3',
    }

    table.put(rk, {'cf1:c1':'v1old'}, timestamp=123)
    table.put(rk, cols)

    table.delete(rk, timestamp=111)
    assert_dict_equal({'cf1:c1':'v1'}, table.row(rk, columns=['cf1:c1']))

    table.delete(rk, ['cf1:c1'], timestamp=111)
    assert_equal({}, table.row(rk, columns=['cf1:c1'], maxversions=2))

    rs = table.row(rk)
    assert_not_in('cf1:c1', rs)
    assert_in('cf1:c2', rs)
    assert_in('cf2:c1', rs)

    table.delete(rk)
    assert_dict_equal({}, table.row(rk))

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
                    #connection.transport.close()
                    raise TException("Fake transport exception")

        for i in range(50):
            with pool.connection() as connection:
                connection.table(TEST_TABLE_NAME)

                try:
                    inner_function()
                except TException:
                    # This error should have been picked up by the
                    # connection pool, and the connection should have
                    # been replaced by a fresh one
                    pass

                connection.table(TEST_TABLE_NAME)

        print("Thread %s done" % name)

    N_THREADS = 10

    with assert_raises(TypeError):
        ConnectionPool(size=[])

    with assert_raises(ValueError):
        ConnectionPool(size=0)

    pool = ConnectionPool(size=3)
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
    pool = ConnectionPool(size=1)

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

@nottest
def test_get():
    rs = table.row('r1')
    assert_is_instance(rs, list)
    assert_equal(len(rs), 1)
    #assert_equal(rs[0])


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

    method_name = 'test_{}'.format(sys.argv[1])
    method = globals()[method_name]
    method()
    
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

from nose.tools import (
    assert_dict_equal,
    assert_equal,
    assert_false,
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
    
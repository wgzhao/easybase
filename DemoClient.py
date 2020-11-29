import os
import random

import easybase

host = os.getenv('HBASE_HOST', 'localhost')
port = int(os.getenv('HBASE_PORT', 9090))
compat = os.getenv('COMPAT', '2.2.0')
# table_name must be exists in hbase 
table_name = os.getenv('HBASE_TABLE', 'easybase_test')

try:
    conn = easybase.Connection(host, port=port, timeout=2000, use_kerberos=False)
except ConnectionRefusedError as e:
    raise SystemError("failed to connection {}:{}, {}".format(host, port, e))

print('list all table in current namespace')
print(conn.tables())

print('list all tables in default namespace')
print(conn.get_tables_by_namespace('default'))

if conn.exist_table(table_name):
    # drop table first
    print("drop table {}".format(table_name))
    conn.delete_table(table_name, disable=True)

# create table 
print("create table {}".format(table_name))
conn.create_table(table_name, {'cf1': dict(), 'cf2': {'max_versions': 2000}})

print("create table {} in namespace: {}".format(table_name, 'eb_ns'))
conn.create_table(table_name, {'cf1': dict()}, ns_name='eb_ns')

tbl = conn.table(table_name)

# first clean up table

# simple put
puts = {'cf1:c1': 'v1', 'cf1:c2': 'v2', 'cf2:c3': 'v3'}
print("write row id = r1 ,", puts)
tbl.put(row='r1', data=puts)

# put with timestamp
ts = 111111
ts2 = 499999999999999999
cnt = 10
vs = 10
print("try to write {} records".format(cnt * 4))
for i in range(cnt):
    # print("write row id = r1 ,",puts,ts + random.randint(10,100))
    tbl.put(row='r1', data=puts, timestamp=ts)
    tbl.put(row='r1', data=puts, timestamp=ts + random.randint(10, 100))
    tbl.put(row='r2', data=puts, timestamp=ts + random.randint(10, 100))
    tbl.put(row='r2', data=puts, timestamp=ts2)

# simple get
print("get row id = r1")
print(tbl.row('r1'))

print("get row id = r1 with 'cf1:c1' and 'cf1:c2'")
print(tbl.row('r1', columns=['cf1:c1', 'cf1:c2']))

# get with timestamp
print("get row id = r1 with timestamp = %d only get 'cf1:c1' column" % ts)
print(tbl.row('r1', columns=['cf1:c1'], timestamp=ts))

# get with time range
print("get row id = r1 and time range between %d and %d" % (ts, ts2))
result = tbl.row('r1', columns=['cf1:c1'], timerange=[ts - 30, ts2], max_versions=vs)
print("get {} records".format(len(result)))
print(result)

# scan rows with time range
print("scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt))
result = tbl.scan(timerange=[ts - 50, ts2], limit=cnt, max_versions=vs)
cnt = 0
for rs in result:
    print(rs)
    cnt += 1
print("total {} records".format(cnt))

# scan rows with time range additional columns
print("scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt))
result = tbl.scan(timerange=[ts - 80, ts2], columns=['cf1:c2'], limit=cnt)

for rs in result:
    print(rs)

print("delete row id = r1 with timestapm = %d" % ts)
tbl.delete('r1', timestamp=ts)

print("delete row id = r2 and ['cf1:c1']")
tbl.delete('r2', ['cf1:c2'])

# version test
print("multi-version test")
tbl.put('r10', puts, timestamp=111)
tbl.put('r10', puts, timestamp=222)

# get 1 record by default
rs = tbl.row('r10', columns=['cf2:c3'], include_timestamp=True)

assert len(rs['cf2:c3']) == 1 and rs['cf2:c3'][0][1] == 222
print(rs)

# get 2 record by specify version
rs = tbl.row('r10', columns=['cf2:c3'], max_versions=10)
assert len(rs['cf2:c3']) == 2
print(rs)

# version-based scan
print("scan with versions")
rs = tbl.scan(row_start='r10', row_stop='r10', columns=['cf2:c3'], max_versions=10)

cnt = 0
for row in rs:
    for item in row[1]['cf2:c3']:
        print("value: {}, timestamp: {}".format(*item))
        cnt += 1

print("should be retrieved 2 records, actually get {} records".format(cnt))

print("test namespace operator")
ns_name = 'eb_ns'
print('create namespace: {}'.format(ns_name))
conn.create_namespace(ns_name)
print('get namespace: {}'.format(ns_name))
res = conn.get_namespace(ns_name)
print(res)
print('delete namespace: {}'.format(ns_name))
conn.delete_namespace(ns_name, cascade=True)

print('list all namespaces')
print(conn.list_namespaces())

print('search table with regex')
print('create table tbl1,tbl2...tbl10')
for i in range(1, 11):
    tbl = 'tbl{}'.format(i)
    if not conn.exist_table(tbl):
        conn.create_table(tbl, {'cf1': dict()})

print('search table which starts with tbl')
tbls = conn.search_table('tbl.*', include_systable=False)
print(tbls)

conn.close()

print("test connection pool")

pool = easybase.ConnectionPool(size=5, host=host, port=port, use_kerberos=True)

with pool.connection() as connect:
    tbl = connect.table(table_name)
    tbl.put(row='r4', data=puts, timestamp=ts)
    print(tbl.row(row='r4'))
    tbl.put(row='r5', data=puts, timestamp=ts)
    print(tbl.row(row='r5'))
    rs = tbl.scan(row_start='r1', row_stop='r2')
    for row in rs:
        print(row)

# try to connect secondly with kerberos
with pool.connection() as connect:
    tbl = connect.table(table_name)
    tbl.put(row='r6', data=puts, timestamp=ts)
    print(tbl.row(row='r6'))
    tbl.put(row='r7', data=puts, timestamp=ts)
    print(tbl.row(row='r7'))
    rs = tbl.scan(row_start='r1', row_stop='r3')
    for row in rs:
        print(row)

    connect.delete_table(table_name, disable=True)

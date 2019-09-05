import easybase
import os
import json

host = os.getenv('HBASE_HOST','localhost')
port = os.getenv('HBASE_PORT', 9090)
table_name = os.getenv('HBASE_TABLE','test1')

conn = easybase.Connection(host, port=port, timeout=2000)

tbl = conn.table(table_name)

# simple put
puts={'cf1:c1': 'v1', 'cf1:c2': 'v2', 'cf2:c3': 'v3'}
print("write row id = r1 ,",puts)
tbl.put(row='r1',data=puts)

# put with timestamp
ts=111111
ts2=499999999999999999
cnt = 10
vs = 10
print("write row id = r1 ,",puts,ts)
tbl.put(row='r1', data=puts, timestamp=ts)

print("write row id = r1 ,",puts,ts2)
tbl.put(row='r1', data=puts, timestamp=ts2)

print("write row id = r2, ",puts,ts2)
tbl.put(row='r2', data=puts, timestamp=ts)

print("write more than one record")

# simple get
print("get row id = r1")
print(tbl.row('r1'))

print("get row id = r1 with 'cf1:c1' and 'cf1:c2'")
print(tbl.row('r1', columns=['cf1:c1','cf1:c2'], maxversions=10))

# get with timestamp
print("get row id = r1 with timestamp = %d only get 'cf1:c1' column" % ts)
print(tbl.row('r1', columns=['cf1:c1'], timestamp=ts))

# get with time range
print("get row id = r1 and time range between %d and %d" % (ts,ts2))
result = tbl.row('r1', columns=['cf1:c1'], timerange=[ts - 1000, ts2], maxversions=vs)

print(result)

# scan rows with time range
print("scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt))
result = tbl.scan(timerange=[ts - 1000, ts2], limit=cnt)

for rs in result:
    print(rs)

# scan rows with time range additional columns
print("scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt))
result = tbl.scan(timerange=[ts - 1000, ts2], columns=['cf1:c2'], limit=cnt)

for rs in result:
    print(rs)


print("delete row id = r1 with timestapm = %d" % ts)
tbl.delete('r1',timestamp=ts)

print("delete row id = r2 and ['cf1:c1']")
tbl.delete('r2',['cf1:c2'])

conn.close()

print("test connection pool")

pool = easybase.ConnectionPool(size=5,host=host,port=port)

with pool.connection() as connect:
    tbl = connect.table(table_name)
    tbl.put(row='r4', data=puts, timestamp=ts)
    print(tbl.row(row='r4'))


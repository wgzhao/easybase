import easybase

conn = easybase.Connection('localhost', port=9090, timeout=20000)

table = "test1"
tbl = conn.table(table)

# simple put
puts={'cf1:c1': 'v1', 'cf1:c2': 'v2', 'cf2:c3': 'v3'}
print "write row id = r1 ,",puts
tbl.put(row='r1',data=puts)

# put with timestamp
ts=111111L
ts2=499999999999999999L
cnt = 10
vs = 10
print "write row id = r1 ,",puts,ts
tbl.put(row='r1', data=puts, timestamp=ts)

print "write row id = r1 ,",puts,ts2
tbl.put(row='r1', data=puts, timestamp=ts2)

print "write row id = r2, ",puts,ts2
tbl.put(row='r2', data=puts, timestamp=ts)

print "write more than one record"

# simple get
print "get row id = r1"
print tbl.row('r1')

print "get row id = r1 with 'cf1:c1' and 'cf1:c2'"
print tbl.row('r1', columns=['cf1:c1','cf1:c2'], maxversions=10)

# get with timestamp
print "get row id = r1 with timestamp = %d only get 'cf1:c1' column" % ts
print tbl.row('r1', columns=['cf1:c1'], timestamp=ts)

# get with time range
print "get row id = r1 and time range between %d and %d" % (ts,ts2)
result = tbl.row('r1', columns=['cf1:c1'], timerange=[ts - 1000, ts2], maxversions=vs)

print result

# scan rows with time range
print "scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt)
result = tbl.scan(timerange=[ts - 1000, ts2], limit=cnt)

for rs in result:
    print rs

# scan rows with time range additional columns
print "scan with  time range from %d to %d and limit %d " % (ts, ts2, cnt)
result = tbl.scan(timerange=[ts - 1000, ts2], columns=['cf1:c2'], limit=cnt)

for rs in result:
    print rs


print "delete row id = r1 with timestapm = %d" % ts
tbl.delete('r1',timestamp=ts)

print "delete row id = r2 and ['cf1:c1']"
tbl.delete('r2',['cf1:c2'])


conn.close()

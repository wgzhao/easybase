## EasyBase

![Python package](https://github.com/wgzhao/easybase/workflows/Test/badge.svg)
[![image](https://img.shields.io/pypi/dm/easybase.svg)](https://pypi.org/project/easybase/)
[![image](https://img.shields.io/pypi/v/easybase.svg)](https://pypi.org/project/easybase/)
[![image](https://img.shields.io/pypi/pyversions/easybase.svg)](https://pypi.org/project/easybase/)
[![image](https://img.shields.io/pypi/implementation/easybase.svg)](https://pypi.org/project/easybase/)

**EasyBase** is a developer-friendly Python library to interact with [Apache HBase](https://hbase.apache.org) . 

The orignal source code forked from [HappyBase](https://github.com/wbolster/happybase).

## Feature highlight

- easy using
- support HBase Thrift 2 protocol
- using [thriftpy2](http://github.com/thriftpy/thriftpy2) instead of old thriftpy

## Installation

```shell
pip install easybase
```

## Usage

### Connect

```python
import easybase
tbl = "test1"
conn = easybase.Connection(host="localhost", port=9000)
table = conn.table(tbl)
rs = table.scan(limit=10)
for row in rs:
  print(row)
```

### Create Table

```python
table_def = {'cf1':dict(),
             'cf2':{'max_versions':2000}}
conn.create_table('test1', table_def)
```

### Write row to table

```python
puts = {'cf1:c1': 'v1',
        'cf1:c2': 'v2',
       'cf2:c2': 'v3'}
tbl = conn.table('test1')
tbl.put(row='rk1', puts)
```

### Get row from table

```python
rk = 'rk1'
tbl = conn.table('test1')
rs = tbl.row(rk)
```

### Scan rows

```python
tbl = conn.table('test1')
scanner = tbl.scan(row_start='rk_0001', row_stop='rk_0100')
for row in scanner:
  print(row)
```

You can get detail in [DemoClient.py](https://github.com/wgzhao/easybase/blob/master/DemoClient.py)

## License

MIT License <http://www.opensource.org/licenses/MIT>.

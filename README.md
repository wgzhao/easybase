# HappyBase


**HappyBase** is a developer-friendly Python library to interact with Apache
HBase.  But it not support Thrift 2 procotol. So I fork it and try to support Thrift 2

* `Documentation <http://happybase.readthedocs.org/>`_ (Read the Docs)
* `Downloads <http://pypi.python.org/pypi/happybase/>`_ (PyPI)
* `Source code <https://github.com/wbolster/happybase>`_ (Github)

.. _Python: http://python.org/
.. _HBase: http://hbase.apache.org/

.. If you're reading this from the README.rst file in a source tree,
   you can generate the HTML documentation by running "make doc" and browsing
   to doc/build/html/index.html to see the result.


.. image:: https://d2weczhvl823v0.cloudfront.net/wbolster/happybase/trend.png
   :alt: Bitdeli badge
   :target: https://bitdeli.com/free



# Thrift 2 protocol class
```
hbase_thrift.TAppend           hbase_thrift.TColumnValue      hbase_thrift.THBaseService     hbase_thrift.TPut
hbase_thrift.TAuthorization    hbase_thrift.TDelete           hbase_thrift.TIOError          hbase_thrift.TResult
hbase_thrift.TCellVisibility   hbase_thrift.TDeleteType       hbase_thrift.TIllegalArgument  hbase_thrift.TRowMutations
hbase_thrift.TColumn           hbase_thrift.TDurability       hbase_thrift.TIncrement        hbase_thrift.TScan
hbase_thrift.TColumnIncrement  hbase_thrift.TGet              hbase_thrift.TMutation         hbase_thrift.TTimeRange
```
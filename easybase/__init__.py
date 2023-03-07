"""
EasyBase, a developer-friendly Python library to interact with Apache
HBase. Support Time-Range scan and multi-version access
"""
# using thriftpy2 server as thrift bridge directly
from pkg_resources import resource_filename
import thriftpy2

thriftpy2.load(
    resource_filename('easybase', 'HBase.thrift'),
    module_name='HBase_thrift'
)

from io import BytesIO
from struct import pack, unpack

import six
from thriftpy2.transport import TTransportBase, TTransportException
try:
    from thriftpy2.transport import readall
except ImportError:
    from thriftpy2.transport.base import readall
from puresasl.client import SASLClient


class TSaslClientTransport(TTransportBase):
    """
    SASL transport
    """

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, transport, host, service, mechanism=six.u('GSSAPI'),
                 **sasl_kwargs):
        """
        transport: an underlying transport to use, typically just a TSocket
        host: the name of the server, from a SASL perspective
        service: the name of the server's service, from a SASL perspective
        mechanism: the name of the preferred mechanism to use
        All other kwargs will be passed to the puresasl.client.SASLClient
        constructor.
        """

        self.transport = transport

        # if six.PY3:
        #     self._patch_pure_sasl()
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = BytesIO()
        self.__rbuf = BytesIO()

    # def _patch_pure_sasl(self):
    #     ''' we need to patch pure_sasl to support python 3 '''
    #     puresasl.mechanisms.mechanisms['GSSAPI'] = CustomGSSAPIMechanism

    def is_open(self):
        return self.transport.is_open() and bool(self.sasl)

    def open(self):
        if not self.transport.is_open():
            self.transport.open()

        self.send_sasl_msg(self.START, self.sasl.mechanism.encode('utf8'))
        self.send_sasl_msg(self.OK, self.sasl.process())

        while True:
            status, challenge = self.recv_sasl_msg()
            if status == self.OK:
                self.send_sasl_msg(self.OK, self.sasl.process(challenge))
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    raise TTransportException(
                        TTransportException.NOT_OPEN,
                        "The server erroneously indicated "
                        "that SASL negotiation was complete")
                else:
                    break
            else:
                raise TTransportException(
                    TTransportException.NOT_OPEN,
                    "Bad SASL negotiation status: %d (%s)"
                    % (status, challenge))

    def send_sasl_msg(self, status, body):
        '''
        body:bytes
        '''
        header = pack(">BI", status, len(body))
        self.transport.write(header + body)
        self.transport.flush()

    def recv_sasl_msg(self):
        header = readall(self.transport.read, 5)
        status, length = unpack(">BI", header)
        if length > 0:
            payload = readall(self.transport.read, length)
        else:
            payload = ""
        return status, payload

    def write(self, data):
        self.__wbuf.write(data)

    def flush(self):
        data = self.__wbuf.getvalue()
        encoded = self.sasl.wrap(data)
        if six.PY2:
            self.transport.write(''.join([
                pack("!i", len(encoded)),
                encoded
            ])
            )
        else:
            self.transport.write(b''.join((pack("!i", len(encoded)), encoded)))
        self.transport.flush()
        self.__wbuf = BytesIO()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0 or sz == 0:
            return ret

        self._read_frame()
        return self.__rbuf.read(sz)

    def _read_frame(self):
        header = readall(self.transport.read, 4)
        length, = unpack('!i', header)
        encoded = readall(self.transport.read, length)
        self.__rbuf = BytesIO(self.sasl.unwrap(encoded))

    def close(self):
        self.sasl.dispose()
        self.transport.close()

    def get_transport(self, trans):
        return self.transport(trans)

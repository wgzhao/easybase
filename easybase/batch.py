"""
EasyBase Batch module.
"""

from collections import defaultdict
import logging
from numbers import Integral

from hbase.ttypes import TMutation, TRowMutations, TPut, TColumnValue

logger = logging.getLogger(__name__)


class Batch(object):
    """Batch mutation class.

    This class cannot be instantiated directly; use :py:meth:`Table.batch`
    instead.
    """
    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=True):
        """Initialise a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, Integral)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' cannot be used when "
                                "'batch_size' is specified")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be > 0")

        self._table = table
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
        self._wal = wal
        self._families = None
        self._reset_mutations()

    def _reset_mutations(self):
        """Reset the internal mutation buffer."""
        self._mutations = defaultdict(list)
        self._mutation_count = 0

    def send(self):
        """Send the batch to the server."""
        bms = [TRowMutations(row, m) for row, m in self._mutations.iteritems()]
        if not bms:
            return

        logger.debug("Sending batch for '%s' (%d mutations on %d rows)",
                     self._table.name, self._mutation_count, len(bms))
        if self._timestamp is None:
            self._table.connection.client.mutateRow(self._table.name, bms[0])
        else:
            self._table.connection.client.mutateRowTs(
                self._table.name, self._mutations, self._timestamp)

        self._reset_mutations()

    #
    # Mutation methods
    #

    def put(self, row, data, wal=None):
        """Store data in the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        if wal is None:
            wal = self._wal
        cols = []

        for column,value in data.iteritems():
            f,q = column.split(":")
            cols.append(TColumnValue(family=f,qualifier=q,value=value))
        tput = TPut(row=row,columnValues=cols,durability=wal,timestamp=self._timestamp)
        self._table.connection.client.put(self._table.name,tput)
        # self._mutations[row].extend(TPut(row=row,columnValues=cols,durability=wal))
        # self._mutation_count += len(data)
        # if self._batch_size and self._mutation_count >= self._batch_size:
        #     self.send()


    def __enter__(self):
        """Called upon entering a ``with`` block"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called upon exiting a ``with`` block"""
        # If the 'with' block raises an exception, the batch will not be
        # sent to the server.
        if self._transaction and exc_type is not None:
            return

        self.send()

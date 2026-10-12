from freethreading import RLock
import psycopg
from pygrametl import ConnectionWrapper

__all__ = ["Value", "PicklableConnectionWrapper"]

class Value:
    """Drop-in threading replacement for multiprocessing.Value."""
    def __init__(self, typecode: str, value=0):
        self.typecode = typecode  # Retained for API compatibility
        self._value = value
        self._lock = RLock()

    def get_lock(self):
        """Returns the underlying lock, matching multiprocessing.Value.get_lock()."""
        return self._lock

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, val):
        with self._lock:
            self._value = val

class PicklableConnectionWrapper(ConnectionWrapper):
    """A ConnectionWrapper that can be pickled and recreated later."""

    def __init__(
        self,
        connection,
        stmtcachesize=1000,
        paramstyle=None,
        copyintonew=False,
    ):
        self._connection = connection
        self._stmtcachesize = stmtcachesize
        self._paramstyle = paramstyle
        self._copyintonew = copyintonew
        super().__init__(
            self._connect(),
            stmtcachesize=stmtcachesize,
            paramstyle=paramstyle,
            copyintonew=copyintonew,
        )

    def _connect(self):
        connection = psycopg.connect(**self._connection.info.get_parameters())
        connection.autocommit = False
        return connection

    def __getstate__(self):
        return {
            "_stmtcachesize": self._stmtcachesize,
            "_paramstyle": self._paramstyle,
            "_copyintonew": self._copyintonew,
        }

    def __setstate__(self, state):
        self._stmtcachesize = state["_stmtcachesize"]
        self._paramstyle = state["_paramstyle"]
        self._copyintonew = state["_copyintonew"]
        ConnectionWrapper.__init__(
            self,
            self._connect(),
            stmtcachesize=self._stmtcachesize,
            paramstyle=self._paramstyle,
            copyintonew=self._copyintonew,
        )

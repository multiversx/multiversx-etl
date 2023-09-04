import sys
import threading


class AtomicCounter(object):
    def __init__(self,
                 value: int = 0,
                 upper_checkpoint_value: int = sys.maxsize,
                 lower_checkpoint_value: int = -sys.maxsize):
        self._value = value
        self._lock = threading.Lock()

        self.upper_checkpoint_value = upper_checkpoint_value
        self.lower_checkpoint_value = lower_checkpoint_value
        self.has_reached_upper_checkpoint: threading.Event = threading.Event()
        self.has_reached_lower_checkpoint: threading.Event = threading.Event()

    def increment(self, amount: int = 1):
        with self._lock:
            self._value += amount

            if self._value >= self.upper_checkpoint_value:
                self.has_reached_upper_checkpoint.set()

            return self._value

    def decrement(self, amount: int = 1):
        with self._lock:
            self._value -= amount

            if self._value <= self.lower_checkpoint_value:
                self.has_reached_lower_checkpoint.set()

            return self._value

    def get_value(self):
        with self._lock:
            return self._value

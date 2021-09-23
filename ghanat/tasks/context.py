import threading
from typing import Any
from contextlib import contextmanager

class Context:
    """
    ctx = Context()

    with ctx(pipeline='test') as flow:
        print('test', flow)
    """
    def __init__(self, **kwargs: Any):
        self.allowed_keys = set(['pipeline','task', 'configs'])
        self._context = threading.local()
        self._context.__dict__.update((key, value) for key, value in kwargs.items() if key in self.allowed_keys)

    def __getattr__(self, name):
        return self._context.__dict__[name]
    
    @contextmanager
    def __call__(self, is_override: bool = False, **kwargs: Any):
        if not is_override:
            self._context.__dict__.update(dict((key, value) for key, value in kwargs.items() if key in self.allowed_keys))
        yield self
        self._context.__dict__.clear()
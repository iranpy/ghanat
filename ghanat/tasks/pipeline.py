from contextlib import contextmanager
from typing import Iterator

from ghanat.tasks.context import ctx


class Pipeline(object):
    def __init__(self, name, schedule, executor):
        self._ctx = None
        self._tasks = set()
    
    def add_task(self, task):
        self._tasks.add(task)

    @contextmanager
    def _flow_context(self) -> Iterator["Pipeline"]:
        """
        When entering a flow context, the Prefect context is modified to include:
            - `flow`: the flow itself
        """
        with ctx(flow=self):
            yield self

    def __enter__(self) -> "Pipeline":
        self._ctx = self._flow_context()
        return self._ctx.__enter__()
        # with ctx(flow=self):
            # yield self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore
        self._ctx.__exit__(exc_type, exc_value, traceback)
        # delete _ctx because it's an active generator, which prevents pickling
        del self._ctx
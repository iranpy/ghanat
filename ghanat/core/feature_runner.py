from ghanat.schema.dataschema import DataSchema
from typing import Callable, List, Dict

from ghanat.source import Source
from ghanat.tasks import Scheduler


class FeatureRunner:
    def __init__(
        self,
        source: Dict[str, Source],
        online: bool,
        offline: bool,
        schedule: Scheduler,
        ttl: str,
        tags: List[str],
        description: str,
        schema: DataSchema,
    ) -> None:
        self.source = source
        self.online = online
        self.offline = offline
        self.schedule = schedule
        self.ttl = ttl
        self.tags = tags
        self.description = description
        self.schema = schema

        self._data = {}

    def _register(self) -> None:
        """Register this runner instance"""
        pass
    
    def _load_source(self) -> None:
        """Iterate over source list and load it's data into a dict of data on self._data"""
        for key, value in self.source.items():
            self._data[key] = value.load()

    def run(self, func: Callable):
        self._register()
        self._load_source()

        transformed = func(**self._data)

        transformed = self.schema(transformed)
        self._data = transformed.dumps

        return self._data
    
    @property
    def data(self):
        """
        return loaded (or transformed) data from sources
        must call after run method
        """
        return self._data
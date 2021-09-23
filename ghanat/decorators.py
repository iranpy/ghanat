import functools
from typing import Callable, List, Dict

from ghanat.source.base import Source
from ghanat.tasks.schedule import Scheduler
from ghanat.core.feature_runner import FeatureRunner
from ghanat.schema.dataschema import DataSchema


def feature_runner(
    source: Dict[str, Source] = {},
    online: bool = False,
    offline: bool = True,
    schedule: Scheduler = None,
    ttl: str = None,
    tags: List[str] = [],
    description: str = '',
    schema: DataSchema = None,
) -> Callable:
    def inner(func):
        @functools.wraps(func)
        def runner(*args, **kwargs):
            fr = FeatureRunner(
                source,
                online,
                offline,
                schedule,
                ttl,
                tags,
                description,
                schema,
            )
            return fr.run(func)
        return runner
    return inner

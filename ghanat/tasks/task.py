from ghanat.tasks.context import ctx


class Task(object):
    def __init__(self, name, task_id, max_retries, retry_delay, timeout, trigger, cache_time, state_handlers):
        self.name = name
        self.task_id = task_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.trigger = trigger
        self.cache_time = cache_time
        self.state_handlers = state_handlers

    def __repr__(self) -> str:
        return "<Task: {self.name}>".format(self=self)

    def prepaire_task_pipeline(self, pipeline: str = None, upstream_task=None):
        pipeline = pipeline or ctx.get("pipeline", None)
        if not pipeline:
            raise ValueError(
                "No Pipeline was passed, and could not infer an active Pipeline context."
            )

    def set_upstream(self):
        pass

    def dependencies_task(self):
        pass

    def upstream_task(self):
        pass

    def downstream_task(self):
        pass
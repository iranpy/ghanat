from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pytz
from typing import Iterator, Optional

from croniter import croniter
import pendulum

from .exceptions import InvalidValueException, ParsingException


class Scheduler(ABC):
    @property
    @abstractmethod
    def event(self) -> Iterator[datetime]:
        ...


class Schedule:
    """
    Cron Scheduler.

    Examples:
        >>> from datetime import datetime, timedelta
        >>> start_time = datetime(2040, 8, 1, 12, 43, 0)  + timedelta(minutes=5)
        >>> s = Schedule('* * * * *', start_time)
        >>> next(s.event)
        DateTime(2040, 8, 1, 12, 49, 0, tzinfo=Timezone('UTC'))

    Args:
        - cron (str): a valid cron string
        - start_date (datetime): start date for the schedule
        - end_date (datetime, optional): an optional end date for the schedule
        - day_or (bool, optional): Control how croniter handles `day` and `day_of_week` entries.
            Defaults to True, matching cron which connects those values using OR.
            If the switch is set to False, the values are connected using AND. This behaves like
            fcron and enables you to e.g. define a job that executes each 2nd friday of a month
            by setting the days of month and the weekday.

    Raises:
        - ValueError: if the cron string is invalid
    """
    def __init__(
        self,
        cron: str,
        start_date: datetime = None,
        end_date: Optional[datetime] = None,
        day_or: bool = True,
    ) -> None:

        if not (cron and start_date):
            raise InvalidValueException('cron string and start_execution_datetime must be specified.')

        if not croniter.is_valid(cron):
            raise ParsingException("Invalid cron string: {}".format(cron))

        self._cron = cron
        valid_min_time = pendulum.instance(datetime.utcnow() + timedelta(0, 60))

        if not hasattr(start_date, 'tz'):
            start_date = start_date.replace(tzinfo=pytz.UTC)

        if valid_min_time > start_date:
            raise InvalidValueException('start_datetime must be at least 1 minute in the future.')

        self._start_date = pendulum.instance(start_date)
        self._tz = getattr(self._start_date, 'tz')

        if end_date is not None:
            end_date = pendulum.instance(end_date)
        self._end_date = end_date
        self._day_or = day_or

    @property
    def event(self) -> Iterator[datetime]:
        start_localized = pytz.timezone(self._tz.name).localize(
            datetime(
                year=self._start_date.year,
                month=self._start_date.month,
                day=self._start_date.day,
                hour=self._start_date.hour,
                minute=self._start_date.minute,
                second=self._start_date.second,
                microsecond=self._start_date.microsecond,
            )
        )

        # Respect microseconds by rounding up
        if start_localized.microsecond:
            start_localized = start_localized + timedelta(seconds=1)

        cron = croniter(self._cron, start_time=start_localized, day_or=self._day_or)

        while True:
            next_date = pendulum.instance(cron.get_next(datetime), self._tz)

            if self._end_date and next_date > self._end_date:
                break

            yield next_date


from datetime import datetime, timedelta

import pytest
import pendulum

from ghanat.tasks.schedule import Schedule
from ghanat.tasks.exceptions import InvalidValueException, ParsingException


def test_schedule_raise_invalid_value():
    with pytest.raises(InvalidValueException):
        Schedule('', datetime.now())


def test_schedule_raise_invalid_cron():
    with pytest.raises(ParsingException):
        Schedule('* invalid_str * * *', datetime.now())


def test_start_date_native():
    start_time = datetime.now() + timedelta(minutes=5)

    s = Schedule('* * * * *', start_time)
    assert next(s.event)


def test_start_date_with_tz():
    start_time = pendulum.instance(datetime.utcnow()) + timedelta(seconds=61)

    s = Schedule(cron='* * * * *', start_date=start_time)
    assert next(s.event)


def test_next_minute():
    start_time = datetime.now() + timedelta(minutes=5)
    next_time = start_time + timedelta(minutes=1)

    s = Schedule('* * * * *', start_time)
    nxt = next(s.event)
    assert nxt.minute == next_time.minute and nxt.hour == next_time.hour


def test_iterator_len():
    start_time = datetime.now() + timedelta(minutes=5)
    end_date = start_time + timedelta(days=4)

    s = Schedule('5 4 * * *', start_time, end_date)
    result = []
    for i in s.event:
        result.append(i)
    assert len(result) == 4

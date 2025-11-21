import time
from datetime import datetime


def ts_to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts)


def dt_to_ts(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()))


def safe_get(d: dict, *keys, default=None):
    v = d
    for k in keys:
        if not isinstance(v, dict) or k not in v:
            return default
        v = v[k]
    return v

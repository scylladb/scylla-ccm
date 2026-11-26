import inspect

import requests
from botocore.client import Config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def s3_config(**kwargs) -> Config:
    # DNS/network blips can last a minute; botocore's default retries give up in ~10s.
    # A fresh dict each call: botocore rewrites the retries dict in place.
    return Config(retries={'max_attempts': 10, 'mode': 'standard'}, **kwargs)


def http_session() -> requests.Session:
    # raise_on_status=False: return the last 5xx so callers still see HTTPError, not RetryError.
    # respect_retry_after_header=False: a server's Retry-After isn't capped by backoff_max.
    # urllib3 1.x (pulled in by botocore on Python 3.9) has no backoff_max; it caps at 120s.
    cap = {'backoff_max': 30} if 'backoff_max' in inspect.signature(Retry).parameters else {}
    retry = Retry(total=8, backoff_factor=1, status_forcelist=(500, 502, 503, 504),
                  raise_on_status=False, allowed_methods=None, respect_retry_after_header=False, **cap)
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


def http_get(url, timeout=60, **kwargs) -> requests.Response:
    return http_session().get(url, timeout=timeout, **kwargs)

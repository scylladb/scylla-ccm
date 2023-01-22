from packaging.version import parse, Version


def parse_version(v: str) -> Version:
    v = v.replace('~', '-')
    return parse(v)

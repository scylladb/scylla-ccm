import errno
import fcntl
import io
import os
import select
import struct

from contextlib import contextmanager
from ctypes import CDLL, get_errno, c_int
from ctypes.util import find_library
from dataclasses import dataclass
from enum import IntEnum

from termios import FIONREAD
from time import sleep
from typing import Callable, Generator, List, Optional, Tuple


class Libc:
    """a delegate class for accessing libc functions not exposed by CPython
    """
    def __init__(self) -> None:
        self.libc = CDLL(find_library('c'), use_errno=True)

    @staticmethod
    def _retry_on_eintr(func: Callable) -> Callable:
        """Wrapper around a libc function and retries on EINTR."""
        def wrapper(*args, **kwargs):
            while True:
                rc = func(*args, **kwargs)
                if rc != -1:
                    return rc
                ec = get_errno()
                if ec != errno.EINTR:
                    raise OSError(errno, os.strerror(ec))

        return wrapper

    def __getattr__(self, name: str) -> Callable:
        return self._retry_on_eintr(getattr(self.libc, name))


class Flag(IntEnum):
    """IN_* flags"""
    ACCESS = 0x00000001
    MODIFY = 0x00000002
    ATTRIB = 0x00000004
    CLOSE_WRITE = 0x00000008
    CLOSE_NOWRITE = 0x00000010
    OPEN = 0x00000020
    MOVED_FROM = 0x00000040
    MOVED_TO = 0x00000080
    CREATE = 0x00000100
    DELETE = 0x00000200
    DELETE_SELF = 0x00000400
    MOVE_SELF = 0x00000800

    UNMOUNT = 0x00002000
    Q_OVERFLOW = 0x00004000
    IGNORED = 0x00008000

    ONLYDIR = 0x01000000
    DONT_FOLLOW = 0x02000000
    EXCL_UNLINK = 0x04000000
    MASK_ADD = 0x20000000
    ISDIR = 0x40000000
    ONESHOT = 0x80000000


@dataclass
class Event:
    """represents a notification"""
    wd: int
    flags: List[Flag]
    cookie: int
    name: str

    _EVENT_FMT = 'iIII'
    _EVENT_SIZE = struct.calcsize(_EVENT_FMT)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int) -> Tuple["Event", int]:
        wd, mask, cookie, namesize = struct.unpack_from(cls._EVENT_FMT,
                                                        data,
                                                        offset)
        name = data[offset - namesize: offset].split(b'\x00', 1)[0]
        offset += cls._EVENT_SIZE + namesize
        flags = [flag for flag in Flag.__members__.values() if flag & mask]
        return cls(wd, flags, cookie, os.fsdecode(name)), offset


class INotify(io.FileIO):
    fd = property(io.FileIO.fileno)

    _libc = CDLL(find_library('c'), use_errno=True)

    def __init__(self) -> None:
        self._libc = Libc()
        flags = os.O_CLOEXEC | os.O_NONBLOCK
        super().__init__(self._libc.inotify_init1(flags), mode='rb')
        self._poller = select.poll()
        self._poller.register(self.fileno())

    @contextmanager
    def watch(self, path, mask):
        """watch the given path"""
        wd = self._libc.inotify_add_watch(self.fileno(),
                                          os.fsencode(path),
                                          mask)
        yield self
        self._libc.inotify_rm_watch(self.fileno(), wd)

    def _read_all(self):
        bytes_avail = c_int()
        fcntl.ioctl(self, FIONREAD, bytes_avail)
        if bytes_avail.value:
            return os.read(self.fileno(), bytes_avail.value)
        return b''

    def read(self,
             timeout: Optional[int] = None,
             read_delay: Optional[int] = None) -> Generator[Event, None, None]:
        """read events from the watched fd
        """
        data = self._read_all()
        if not data:
            if timeout == 0:
                return
            if not self._poller.poll(timeout):
                return
            if read_delay is not None:
                sleep(read_delay / 1000.0)
            data = self._read_all()
        offset = 0
        while offset < len(data):
            event, offset = Event.from_bytes(data, offset)
            yield event

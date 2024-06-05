from ccmlib.utils.inotify import INotify, Flag


def test_watch_modify(tmp_path):
    log_file = tmp_path / "foo.log"
    log_file.write_text("hello")
    notify = INotify()
    with notify.watch(log_file.as_posix(), Flag.MODIFY):
        log_file.write_text("something new")
        ev = None
        for e in notify.read():
            ev = e
        assert ev is not None
        assert Flag.MODIFY in ev.flags


def test_watch_move(tmp_path):
    log_file = tmp_path / "from.log"
    log_file.write_text("hello")
    notify = INotify()
    with notify.watch(log_file.as_posix(), Flag.MOVE_SELF):
        log_file.rename(tmp_path / "to.log")
        ev = None
        for e in notify.read():
            ev = e
        assert ev is not None
        assert Flag.MOVE_SELF in ev.flags

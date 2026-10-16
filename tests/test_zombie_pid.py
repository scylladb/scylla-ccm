import subprocess
import time

from ccmlib.node import Node, Status, _reap_if_zombie


def test_exited_unreaped_child_is_dead():
    p = subprocess.Popen(['true'])
    for _ in range(50):
        if _reap_if_zombie(p.pid):
            break
        time.sleep(0.1)
    else:
        raise AssertionError('child never seen as zombie')
    assert not _reap_if_zombie(p.pid)  # reaped, pid gone


def _node_up(pid):
    node = Node.__new__(Node)
    node.pid, node.status = pid, Status.UP
    node._update_config = lambda: None
    return node


def _wait_zombie(pid):
    for _ in range(50):
        with open(f'/proc/{pid}/stat') as f:
            if f.read().rpartition(')')[2].split()[0] == 'Z':
                return
        time.sleep(0.1)
    raise AssertionError('process never became a zombie')


def test_update_status_marks_zombie_child_down():
    p = subprocess.Popen(['true'])
    _wait_zombie(p.pid)
    node = _node_up(p.pid)
    node._Node__update_status()
    assert node.status == Status.DOWN and node.pid is None


def test_update_status_marks_zombie_of_other_parent_down():
    # zombie child of a different (still alive) process: waitpid raises ChildProcessError
    sh = subprocess.Popen(['sh', '-c', 'true & echo $!; sleep 5'], stdout=subprocess.PIPE, text=True)
    try:
        pid = int(sh.stdout.readline())
        _wait_zombie(pid)
        node = _node_up(pid)
        node._Node__update_status()
        assert node.status == Status.DOWN
    finally:
        sh.kill()
        sh.wait()

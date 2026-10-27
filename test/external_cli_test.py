from pulsar.managers import status
from pulsar.managers.queued_external_cli import ExternalCliQueueManager
from pulsar.managers.util.cli.shell.local import LocalShell
from .test_utils import (
    minimal_app_for_managers,
    temp_directory,
)


class RecordingShell:

    def __init__(self):
        self.calls = []

    def execute(self, cmd, **kwds):
        self.calls.append((cmd, kwds))
        return type("Out", (), {"stdout": "", "stderr": "", "returncode": 0})()


def _manager(**kwds):
    app = minimal_app_for_managers()
    return ExternalCliQueueManager("test", app, job_plugin="Slurm", **kwds)


def test_sudo_command_wraps_scheduler_command():
    with temp_directory():
        manager = _manager()
        cmd = manager._sudo_command("sbatch /tmp/job/command.sh", "u1")
        assert cmd[-2:] == ["sbatch", "/tmp/job/command.sh"]
        assert cmd[1:5] == ["-n", "-E", "-u", "u1"]


def test_remote_shell_rejected():
    try:
        _manager(shell_plugin="SecureShell", shell_hostname="x")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_invalid_user_rejected_and_user_required():
    manager = _manager()
    for params in ({}, {"user": "-u"}, {"user": "#0"}, {"user": "a b"}):
        try:
            manager._prepare_submission("1", params)
        except Exception:
            continue
        raise AssertionError(f"accepted {params}")


def test_kill_runs_as_mapped_user():
    manager = _manager()
    manager.user_map["42"] = "u1"
    shell = RecordingShell()
    manager._execute_kill(shell, "scancel 42", "42")
    cmd, _ = shell.calls[0]
    assert cmd[-2:] == ["scancel", "42"]
    assert cmd[3:5] == ["-u", "u1"]


def test_reclaim_after_cancel_waits_for_scheduler():
    manager = _manager()
    manager._external_ids["1"] = "42"
    manager._was_cancelled = lambda job_id: True
    chowned = []
    manager._change_ownership = lambda job_id, user: chowned.append((job_id, user))
    states = [status.RUNNING, status.COMPLETE]
    manager._get_status_external = lambda external_id: states.pop(0)
    assert manager.get_status("1") == status.CANCELLED
    assert not chowned
    assert manager.get_status("1") == status.CANCELLED
    assert len(chowned) == 1
    manager.get_status("1")
    assert len(chowned) == 1


def test_local_shell_cwd():
    with temp_directory() as d:
        out = LocalShell().execute(["pwd"], cwd=d)
        assert out.stdout.strip() == d

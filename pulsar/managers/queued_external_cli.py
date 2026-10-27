"""
Pulsar job manager that uses a CLI interface to a job queue (e.g. SLURM's
sbatch, scancel, etc...) and runs jobs as the real user requested by the client.

This is the CLI counterpart of ``queued_external_drmaa``. The job directory is
chowned to the (optionally mapped) user, the submission and kill commands are
run through ``sudo -u <user>`` and the directory is handed back to the Pulsar
user once the job has finished. Status queries are run as the Pulsar user.
"""

import shlex
from getpass import getuser
from logging import getLogger
from typing import (
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
)

from pulsar.managers import status
from .queued_cli import CliQueueManager
from .util.run_as_user import (
    RunAsUserMixin,
    VALID_MAPPED_USER,
)
from .util.sudo import (
    SUDO_PATH,
    SUDO_PRESERVE_ENVIRONMENT_ARG,
    SUDO_USER_ARG,
)

if TYPE_CHECKING:
    from pulsar.core import PulsarApp
    from pulsar.managers.status import StateLiteral

log = getLogger(__name__)

JOB_FILE_USER = "user"
SUDO_NON_INTERACTIVE_ARG = "-n"


class ExternalCliQueueManager(RunAsUserMixin, CliQueueManager):
    manager_type = "queued_external_cli"

    def __init__(self, name: str, app: "PulsarApp", **kwds):
        super().__init__(name, app, **kwds)
        if self.shell_params.get("plugin", "LocalShell") != "LocalShell":
            raise ValueError(
                "queued_external_cli requires the LocalShell shell plugin, it "
                "cannot be combined with a remote shell."
            )
        self._init_run_as_user(kwds)
        self.reclaimed: Dict[str, bool] = {}
        self.user_map: Dict[str, str] = {}
        self._job_users: Dict[str, str] = {}

    def _prepare_submission(self, job_id: str, submit_params: Dict[str, str]) -> None:
        user = submit_params.get("user")
        log.info("Submit as user %s", user)
        if not user:
            raise Exception("Must specify user submit parameter with this manager.")
        user = self._map_user(user)
        if not VALID_MAPPED_USER.fullmatch(user):
            # Without a mapping script the name is the client's, and it is about
            # to reach `sudo -u`, so it is held to the same standard.
            raise Exception("Invalid user name supplied for job")
        self._change_ownership(job_id, user)
        self._job_users[job_id] = user
        self._job_directory(job_id).store_metadata(JOB_FILE_USER, user)

    def _sudo_command(self, command: str, user: str) -> List[str]:
        return [
            SUDO_PATH,
            SUDO_NON_INTERACTIVE_ARG,
            SUDO_PRESERVE_ENVIRONMENT_ARG,
            SUDO_USER_ARG,
            user,
        ] + shlex.split(command)

    def _execute_submission(self, shell, submission_command: str, job_id: str):
        user = self._job_users[job_id]
        # Run from the job directory: the user can read it, and the scheduler
        # may use the submission directory as the job's working directory.
        return shell.execute(
            self._sudo_command(submission_command, user),
            cwd=str(self._job_directory(job_id).path),
        )

    def _register_external_id(self, job_id, external_id):
        external_id = super()._register_external_id(job_id, external_id)
        self.user_map[external_id] = self._job_users[job_id]
        return external_id

    def _execute_kill(self, shell, kill_command: str, external_id: str):
        user = self.user_map[external_id]
        return shell.execute(self._sudo_command(kill_command, user))

    def get_status(self, job_id: str) -> "StateLiteral":
        job_status = super().get_status(job_id)
        external_id = self._external_id(job_id)
        if job_id in self.reclaimed or not external_id:
            return job_status
        # A cancelled job reports CANCELLED straight away, but the scheduler may
        # still be tearing it down, so only hand the directory back once the
        # scheduler reports the job as finished.
        if job_status == status.CANCELLED:
            finished = status.is_job_done(self._get_status_external(external_id))
        else:
            finished = status.is_job_done(job_status)
        if finished:
            # Reclaim the working directory before Pulsar reads terminal job data.
            self.reclaimed[job_id] = True
            self._change_ownership(job_id, getuser())
        return job_status

    def _recover_active_job(self, job_id: str) -> None:
        super()._recover_active_job(job_id)
        user = self._job_directory(job_id).load_metadata(JOB_FILE_USER, None)
        if user:
            self._job_users[job_id] = user
            self.user_map[self._external_ids[job_id]] = user

    def _deactivate_job(self, job_id: str) -> None:
        external_id = self._external_id(job_id)
        if external_id is not None:
            self.user_map.pop(external_id, None)
        self._job_users.pop(job_id, None)
        self.reclaimed.pop(job_id, None)
        super()._deactivate_job(job_id)

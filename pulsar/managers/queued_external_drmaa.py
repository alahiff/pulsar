from getpass import getuser
from json import dumps
from logging import getLogger
from typing import (
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
)

from pulsar.managers.util.env import EnvVar
from .base.base_drmaa import BaseDrmaaManager
from .util.run_as_user import (
    DEFAULT_USER_MAPPING_TIMEOUT,  # noqa: F401
    handle_default_script as _handle_default,
    RunAsUserMixin,
    VALID_MAPPED_USER,  # noqa: F401
)
from ..managers import status

if TYPE_CHECKING:
    from galaxy.tool_util.deps.dependencies import DependenciesDescription

    from pulsar.core import PulsarApp
    from pulsar.managers.status import StateLiteral

log = getLogger(__name__)

DEFAULT_DRMAA_KILL_SCRIPT = "scripts/drmaa_kill.bash"
DEFAULT_DRMAA_LAUNCH_SCRIPT = "scripts/drmaa_launch.bash"


class ExternalDrmaaQueueManager(RunAsUserMixin, BaseDrmaaManager):
    """
    DRMAA backed queue manager.
    """

    manager_type = "queued_external_drmaa"

    def __init__(self, name: str, app: "PulsarApp", **kwds):
        super().__init__(name, app, **kwds)
        self._init_run_as_user(kwds)
        self.drmaa_kill_script = _handle_default(
            kwds.get("drmaa_kill_script"), "drmaa_kill"
        )
        self.drmaa_launch_script = _handle_default(
            kwds.get("drmaa_launch_script"), "drmaa_launch"
        )
        self.reclaimed: Dict[str, bool] = {}
        self.user_map: Dict[str, str] = {}

    def launch(
        self,
        job_id: str,
        command_line: str,
        submit_params: Dict[str, str] = {},
        dependencies_description: Optional["DependenciesDescription"] = None,
        env: List[EnvVar] = [],
        setup_params: Optional[Dict[str, str]] = None,
    ) -> None:
        self._check_execution_with_tool_file(job_id, command_line)
        attributes = self._build_template_attributes(
            job_id,
            command_line,
            dependencies_description=dependencies_description,
            env=env,
            submit_params=submit_params,
            setup_params=setup_params,
        )
        with open(attributes["remoteCommand"]) as fh:
            print(fh.read())
        job_attributes_file = self._write_job_file(job_id, "jt.json", dumps(attributes))
        user = submit_params.get("user")
        log.info("Submit as user %s" % user)
        if not user:
            raise Exception("Must specify user submit parameter with this manager.")
        user = self._map_user(user)
        self._change_ownership(job_id, user)
        external_id = self.__launch(job_attributes_file, user).strip()
        self.user_map[external_id] = user
        self._register_external_id(job_id, external_id)

    def _kill_external(self, external_id: str) -> None:
        user = self.user_map[external_id]
        self._sudo(self.drmaa_kill_script, "--external_id", external_id, user=user)

    def get_status(self, job_id: str) -> "StateLiteral":
        external_id = self._external_id(job_id)
        if not external_id:
            raise KeyError("Failed to find external id for job_id %s" % job_id)
        external_status = super()._get_status_external(external_id)
        # Reclaim the working directory before Pulsar reads terminal job data.
        if status.is_job_done(external_status) and job_id not in self.reclaimed:
            self.reclaimed[job_id] = True
            self._change_ownership(job_id, getuser())
        return external_status

    def __launch(self, job_attributes_file: str, user: str) -> str:
        return self._sudo(
            self.drmaa_launch_script,
            "--job_attributes",
            job_attributes_file,
            user=user,
        )

    def _deactivate_job(self, job_id: str) -> None:
        external_id = self._external_id(job_id)
        if external_id is not None:
            del self.user_map[external_id]
        self.reclaimed.pop(job_id, None)
        super()._deactivate_job(job_id)

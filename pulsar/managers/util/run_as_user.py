"""Helpers shared by managers that run jobs as the real (Galaxy) user.

Such managers use ``sudo`` to chown the job directory to the target user, to
submit or kill the job as that user and to hand the directory back to Pulsar
when the job finishes.
"""

import re
import subprocess
from logging import getLogger
from typing import (
    Any,
    Dict,
    Optional,
)

from galaxy.util import which

from .sudo import sudo_popen

log = getLogger(__name__)

DEFAULT_USER_MAPPING_TIMEOUT = 30

# A mapped username is handed to `sudo -u` and to chown by
# chown_working_directory, so it is constrained to a conservative POSIX
# username. Galaxy applies an equivalent constraint to the names it sends; the
# output of an operator-supplied mapping script carries no such guarantee.
VALID_MAPPED_USER = re.compile(r"[A-Za-z0-9._][A-Za-z0-9._-]*")


def handle_default_script(value: Optional[str], script_name: str) -> str:
    """There are two potential variants of these scripts,
    the Bash scripts that are meant to be run within PULSAR_ROOT
    for older-style installs and the binaries created by setup.py
    as part of a proper pulsar installation.

    This method first looks for the newer style variant of these
    scripts and returns the full path to them if needed and falls
    back to the bash scripts if these cannot be found.
    """
    if value:
        return value

    installed_script = which("pulsar-%s" % script_name.replace("_", "-"))
    if installed_script:
        return installed_script
    else:
        return "scripts/%s.bash" % script_name


class RunAsUserMixin:
    """Mixin for directory based managers that run jobs as the real user."""

    def _init_run_as_user(self, kwds: Dict[str, Any]) -> None:
        self.chown_working_directory_script = handle_default_script(
            kwds.get("chown_working_directory_script"), "chown_working_directory"
        )
        self.user_mapping_script: Optional[str] = kwds.get("user_mapping_script")
        self.user_mapping_timeout = int(
            kwds.get("user_mapping_timeout", DEFAULT_USER_MAPPING_TIMEOUT)
        )
        self.production = str(kwds.get("production", "true")).lower() != "false"

    def _map_user(self, user: str) -> str:
        """Map the username supplied by the client onto one on this system.

        Returns the name unchanged when no mapping script is configured.

        The result is validated rather than trusted: it reaches `sudo -u` and a
        shell command, and unlike the name Galaxy sends it comes from a script
        this manager does not control.
        """
        script = self.user_mapping_script
        if not script:
            return user
        try:
            mapped_user = subprocess.check_output(
                [script, user],
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.user_mapping_timeout,
            ).strip()
        except subprocess.CalledProcessError as e:
            log.error("Could not map user %s: %s", user, e.stderr)
            raise Exception("User mapping script failed")
        except subprocess.TimeoutExpired:
            log.error(
                "User mapping script did not return within %s seconds mapping user %s",
                self.user_mapping_timeout,
                user,
            )
            raise Exception("User mapping script timed out")
        if not VALID_MAPPED_USER.fullmatch(mapped_user):
            # repr keeps a newline or quote in the value from garbling the log.
            log.error(
                "User mapping script returned an unusable username for %s: %r",
                user,
                mapped_user[:64],
            )
            raise Exception("User mapping script returned an invalid username")
        log.info("Mapped user %s to %s", user, mapped_user)
        return mapped_user

    def _change_ownership(self, job_id: str, username: str) -> None:
        cmds = [self.chown_working_directory_script, "--user", username]
        if self.production:
            cmds.extend(["--job_id", job_id])
        else:
            # In testing, the loading working directory from server.ini doesn't
            # work. Need to reimagine how to securely map job_id to working
            # direcotry between test cases and production.
            cmds.extend(["--job_directory", str(self._job_directory(job_id).path)])  # type: ignore[attr-defined]
        # TODO: Verify ownership change.
        self._sudo(*cmds)

    def _sudo(self, *cmds, **kwargs) -> str:
        p = sudo_popen(*cmds, **kwargs)
        stdout, stderr = p.communicate()
        assert p.returncode == 0, f"{stdout}, {stderr}"
        return stdout

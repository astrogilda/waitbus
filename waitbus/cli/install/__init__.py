"""Install commands. In v0.4.0 these are still top-level commands
(install-systemd, install-launchd, install-credentials); the nested
``install <target>`` surface lands in a later refactor.
"""

from .credentials import install_credentials
from .launchd import install_launchd
from .systemd import install_systemd

__all__ = ["install_credentials", "install_launchd", "install_systemd"]

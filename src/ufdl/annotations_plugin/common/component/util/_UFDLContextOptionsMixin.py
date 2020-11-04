from abc import ABC

from ufdl.pythonclient import UFDLServerContext

from wai.annotations.core.util import InstanceState

from wai.common.cli import OptionValueHandler
from wai.common.cli.options import TypedOption


class UFDLContextOptionsMixin(OptionValueHandler, ABC):
    """
    Mixin class for UFDL components which provides the
    options necessary for creating the UFDL context for the
    Python client, and performs the actual instantiation.
    """
    host: str = TypedOption(
        "-h", "--host",
        type=str,
        required=True,
        help="the UFDL server",
        metavar="PROTOCOL://HOST:PORT"
    )

    username: str = TypedOption(
        "-u", "--username",
        type=str,
        required=True,
        help="the username of the user on the UFDL server",
        metavar="USERNAME"
    )

    password: str = TypedOption(
        "-w", "--password",
        type=str,
        required=True,
        help="the password of the user on the UFDL server",
        metavar="PASSWORD"
    )

    # The connection to the UFDL server
    ufdl_context = InstanceState(lambda self: UFDLServerContext(self.host, self.username, self.password))

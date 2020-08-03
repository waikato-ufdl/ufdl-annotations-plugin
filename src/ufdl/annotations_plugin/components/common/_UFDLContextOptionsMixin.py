from argparse import Namespace
from typing import Union, Any

from ufdl.pythonclient import UFDLServerContext

from wai.common.cli import CLIInstantiable, OptionsList
from wai.common.cli.options import TypedOption


class UFDLContextOptionsMixin(CLIInstantiable):
    """
    Mixin class for UFDL components which provides the
    options necessary for creating the UFDL context for the
    Python client, and performs the actual instantiation on
    construction.
    """
    host: str = TypedOption(
        "-h", "--host",
        type=str,
        required=True,
        help="the host-name of UFDL server",
        metavar="HOSTNAME"
    )

    port: int = TypedOption(
        "-p", "--port",
        type=int,
        required=True,
        help="the port number of the UFDL server",
        metavar="PORT"
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

    def __init__(self,
                 _namespace: Union[Namespace, OptionsList, None] = None,
                 **internal: Any):
        super().__init__(_namespace, **internal)

        # Create the client context for connecting to the server
        self._context: UFDLServerContext = UFDLServerContext(self.host, self.port, self.username, self.password)

    @property
    def ufdl_context(self) -> UFDLServerContext:
        """
        Gets the UFDL server context for this object.
        """
        return self._context

from argparse import Namespace
from typing import Union, Any

from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import Exact

from ufdl.pythonclient.functional.core import team, project

from wai.common.cli import OptionsList
from wai.common.cli.options import TypedOption

from ._UFDLContextOptionsMixin import UFDLContextOptionsMixin


class UFDLProjectSpecificMixin(UFDLContextOptionsMixin):
    """
    Mixin class for UFDL components access a particular
    project.
    """
    team: str = TypedOption(
        "--team",
        type=str,
        required=True,
        help="the name of the team that owns the dataset to write to",
        metavar="TEAM"
    )

    project: str = TypedOption(
        "--project",
        type=str,
        required=True,
        help="the name of the project that owns the dataset to write to",
        metavar="PROJECT"
    )

    def __init__(self,
                 _namespace: Union[Namespace, OptionsList, None] = None,
                 **internal: Any):
        super().__init__(_namespace, **internal)

        # Prepare the appropriate project
        self._project_pk = self._get_project()

    @property
    def project_pk(self) -> int:
        """
        Gets the primary key of the project being targeted.
        """
        return self._project_pk

    def _get_project(self) -> int:
        """
        Gets the primary key of the specified project in the specified team.

        :return:    The project's primary key.
        """
        # Get the list of teams with the given name (should be at most one)
        teams = team.list(self.ufdl_context, self._get_team_filter())

        # If none, then the team doesn't exist
        if len(teams) == 0:
            raise Exception(f"No team named '{self.team}'")

        # Get the primary key of the team
        team_pk = teams[0]['pk']

        # Get the list of projects owned by that team, with the given name (should be at most one)
        projects = project.list(self.ufdl_context, self._get_project_filter(team_pk))

        # If none, then the project doesn't exist
        if len(projects) == 0:
            raise Exception(f"No project named '{self.project}'")

        # Get the primary key of the project
        return projects[0]['pk']

    def _get_team_filter(self) -> FilterSpec:
        """
        Gets the filter specification for retrieving the team specified by name.

        :return:    The filter specification.
        """
        return FilterSpec(
            expressions=[
                Exact(field="name", value=self.team)
            ]
        )

    def _get_project_filter(self, team_pk: int) -> FilterSpec:
        """
        Gets the filter specification for retrieving the project specified by name
        and team pk.

        :return:    The filter specification.
        """
        return FilterSpec(
            expressions=[
                Exact(field="name", value=self.project) & Exact(field="team", value=team_pk)
            ]
        )

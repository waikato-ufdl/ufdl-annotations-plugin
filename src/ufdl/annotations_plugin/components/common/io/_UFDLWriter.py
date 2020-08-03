from abc import ABC, abstractmethod
from argparse import Namespace
from typing import Iterable, TypeVar, Optional, Union, Any, Callable, Tuple, List

from ufdl.json.core.filter import FilterSpec, OrderBy
from ufdl.json.core.filter.field import Exact
from ufdl.pythonclient import UFDLServerContext

from ufdl.pythonclient.functional.core import team, project, licence

from wai.annotations.core.component import Writer

from wai.common.cli import OptionsList
from wai.common.cli.options import TypedOption, FlagOption

from wai.json.object import OptionallyPresent
from wai.json.raw import RawJSONArray, RawJSONObject

from .._UFDLContextOptionsMixin import UFDLContextOptionsMixin

DATASET_LIST_METHOD_TYPE = Callable[[UFDLServerContext, Optional[FilterSpec]], RawJSONArray]
DATASET_CREATE_METHOD_TYPE = Callable[[UFDLServerContext, str, int, int, str, bool, str], RawJSONObject]
DATASET_COPY_METHOD_TYPE = Callable[[UFDLServerContext, int, OptionallyPresent[str]], RawJSONObject]

ExternalFormat = TypeVar("ExternalFormat")


class UFDLWriter(Writer[ExternalFormat], UFDLContextOptionsMixin, ABC):
    """
    Writes instances to a data-set on a UFDL server.
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

    dataset: str = TypedOption(
        "--dataset",
        type=str,
        required=True,
        help="the name of the dataset to write to",
        metavar="DATASET"
    )

    version: int = TypedOption(
        "--version",
        type=int,
        help="the version of the dataset to update (omit for new dataset)",
        metavar="VERSION"
    )

    licence: str = TypedOption(
        "--licence",
        type=str,
        help="the licence for the new dataset",
        metavar="LICENCE"
    )

    description: str = TypedOption(
        "--description",
        type=str,
        help="the description for the new dataset",
        metavar="DESCRIPTION"
    )

    is_public: bool = FlagOption(
        "--public",
        help="whether the new dataset should be publicly accessible"
    )

    tags: List[str] = TypedOption(
        "--tags",
        type=str,
        nargs="+",
        help="any tags to apply to the new dataset",
        metavar="TAG"
    )

    create_copy: bool = FlagOption(
        "--create-copy",
        help="whether to copy the dataset before writing"
    )

    new_dataset_per_split: bool = FlagOption(
        "--new-dataset-per-split",
        help="whether to create a unique dataset per split (default is a separate sub-folder per split)"
    )

    def __init__(self,
                 _namespace: Union[Namespace, OptionsList, None] = None,
                 **internal: Any):
        super().__init__(_namespace, **internal)

        # Get the dataset methods to use from the sub-class
        self._list_datasets, self._create_dataset, self._copy_dataset = self.get_dataset_methods()

        # Prepare the appropriate project and dataset for writing
        self._project_pk = self._get_project()
        self._dataset_pk = self._prepare_dataset(self._project_pk)

    @property
    def project_pk(self) -> int:
        """
        Gets the primary key of the project being targeted.
        """
        return self._project_pk

    @property
    def target_dataset(self) -> Optional[int]:
        """
        The target dataset to write to.

        :return:    The dataset's primary key.
        """
        return self._dataset_pk

    @abstractmethod
    def get_dataset_methods(self) -> Tuple[DATASET_LIST_METHOD_TYPE, DATASET_CREATE_METHOD_TYPE, DATASET_COPY_METHOD_TYPE]:
        """
        Gets the list and create methods for the particular type
        of dataset in the writer's domain.

        :return:    The list and create methods.
        """
        pass

    def _prepare_dataset(self, project_pk: int) -> Optional[int]:
        """
        Prepares the dataset to write to.

        :param      The primary key of the project to store the dataset under.
        :return:    The primary key of the dataset to write to.
        """
        # No version provided, creating a new dataset
        if self.version is None:
            # If we are splitting and creating a new dataset per split, don't need a base dataset yet
            if self.is_splitting and self.new_dataset_per_split:
                return None

            return self._create_new_dataset(self.dataset, project_pk)

        # Version provided, updating current dataset
        else:
            # Get the primary key of the dataset to update
            dataset_pk = self._get_existing_dataset(self.dataset, project_pk)

            # If the dataset doesn't exist, error
            if dataset_pk is None:
                raise Exception(f"Version {self.version} of dataset '{self.dataset}' does not exist")

            # If a copy is requested, create it
            if self.create_copy:
                dataset_pk = self._copy_dataset(self.ufdl_context, dataset_pk)['pk']

            return dataset_pk

    def _create_new_dataset(self, name: str, project_pk: int) -> int:
        """
        Creates a new dataset in the given project.

        :param name:        The name to give the dataset.
        :param project_pk:  The project in which to create the new dataset.
        :return:            The primary key of the new dataset.
        """
        # See if there is an existing dataset
        if self._get_existing_dataset(name, project_pk) is not None:
            raise Exception(f"A dataset named '{name}' already exists")

        return self._create_dataset(self.ufdl_context,
                                    name,
                                    project_pk,
                                    self._get_licence(),
                                    self.description if self.description is not None else "",
                                    self.is_public,
                                    ",".join(self.tags)
                                    )['pk']

    def _get_existing_dataset(self, name: str, project_pk: int) -> Optional[int]:
        """
        Gets the primary key of the specified dataset, if it exists,
        or None if it doesn't.

        :param name:        The name of the dataset to look for.
        :param project_pk:  The primary key of the project to look in.
        :return:            Dataset primary key or None.
        """
        # If no version is supplied, we want to create a new dataset with
        # this name, so make sure there isn't one already.
        if self.version is None:
            datasets = self._list_datasets(self.ufdl_context, FilterSpec(
                expressions=[
                    Exact(field="name", value=name) &
                    Exact(field="project", value=project_pk)
                ],
                order_by=[
                    OrderBy(field="version", ascending=False)
                ]
            ))

        # Version is supplied, make sure it exists
        else:
            datasets = self._list_datasets(self.ufdl_context, FilterSpec(
                expressions=[
                    Exact(field="name", value=self.dataset) &
                    Exact(field="version", value=self.version) &
                    Exact(field="project", value=project_pk)
                ]
            ))

        return None if len(datasets) == 0 else datasets[0]['pk']

    def _get_licence(self) -> int:
        """
        Gets the primary key of the specified licence.

        :return:    The licence's primary key.
        """
        # If the licence argument wasn't specified, error
        if self.licence is None:
            raise Exception("No licence specified")

        # Get the licence with the given name
        licences = licence.list(self.ufdl_context, FilterSpec(
            expressions=[
                Exact(field="name", value=self.licence)
            ]
        ))

        # If no licence with that name was found, error
        if len(licences) == 0:
            raise Exception(f"No licence named '{self.licence}'")

        return licences[0]['pk']

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

    def split_write(self, instances: Iterable[ExternalFormat], split_name: str):
        if self.new_dataset_per_split:
            if self.target_dataset is not None:
                self.write_to_dataset(instances,
                                      self._copy_dataset(self.ufdl_context, self.target_dataset, f"{self.dataset}-{split_name}")['pk'],
                                      None)
            else:
                self.write_to_dataset(instances,
                                      self._create_new_dataset(f"{self.dataset}-{split_name}", self.project_pk),
                                      None)
        else:
            self.write_to_dataset(instances, self.target_dataset, split_name)

    def write(self, instances: Iterable[ExternalFormat]):
        self.write_to_dataset(instances, self.target_dataset, None)

    @abstractmethod
    def write_to_dataset(self, instances: Iterable[ExternalFormat], dataset_pk: int, subfolder: Optional[str]):
        """
        Writes the instances to the given dataset.

        :param instances:       The instances to write.
        :param dataset_pk:      The primary key of the target dataset.
        :param subfolder:       The (optional) sub-folder to write into.
        """
        pass

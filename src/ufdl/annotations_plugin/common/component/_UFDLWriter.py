from abc import ABC, abstractmethod
from typing import TypeVar, Optional, List

from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import Exact

from ufdl.pythonclient.functional.core import licence

from wai.annotations.core.component.util import SplitSink, SplitState, RequiresNoSplitFinalisation
from wai.annotations.core.stream.util import ProcessState
from wai.annotations.core.util import InstanceState

from wai.common.cli.options import TypedOption, FlagOption

from ..util import get_existing_dataset
from .util import UFDLProjectSpecificMixin, DatasetMethods

ExternalFormat = TypeVar("ExternalFormat")


class UFDLWriter(
    RequiresNoSplitFinalisation,
    UFDLProjectSpecificMixin,
    SplitSink[ExternalFormat],
    ABC
):
    """
    Writes instances to a data-set on a UFDL server.
    """
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
        help="the version of the dataset to update (omit for latest version)",
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

    on_existing: str = TypedOption(
        "--on-existing",
        type=str,
        choices=("error", "copy", "overwrite"),
        default="copy",
        help="action to take when the specified dataset already exists",
        metavar="ACTION"
    )

    new_dataset_per_split: bool = FlagOption(
        "--new-dataset-per-split",
        help="whether to create a unique dataset per split (default is a separate sub-folder per split)"
    )

    # The data-set the new data-sets will be based on
    source_dataset = ProcessState(lambda self: self._init_source_dataset())

    # The dataset to write each split into
    target_dataset: Optional[int] = SplitState(lambda self: self._init_target_dataset())

    # The sub-folder in the dataset to write into, per split
    target_subfolder: Optional[str] = SplitState(
        lambda self: None if self.new_dataset_per_split or not self.is_splitting else self.split_label
    )

    # The list, create and copy methods for the type of dataset
    dataset_methods: DatasetMethods = InstanceState(lambda self: self.get_dataset_methods())

    # The primary key of the specified licence
    licence_pk: int = ProcessState(lambda self: self._get_licence())

    def consume_element_for_split(self, element: ExternalFormat):
        self.write_to_dataset(element, self.target_dataset, self.target_subfolder)

    @abstractmethod
    def write_to_dataset(self, element: ExternalFormat, dataset_pk: int, subfolder: Optional[str]):
        """
        Writes the instances to the given dataset.

        :param element:         The instance to write.
        :param dataset_pk:      The primary key of the target dataset.
        :param subfolder:       The (optional) sub-folder to write into.
        """
        pass

    @abstractmethod
    def get_dataset_methods(self) -> DatasetMethods:
        """
        Gets the list, create and copy methods for the particular type
        of dataset in the writer's domain.

        :return:    The list, create and copy methods.
        """
        pass

    def _init_source_dataset(self) -> Optional[int]:
        """
        Determines the source data-set for writing/copying/updating, if any.

        :return:    The primary key of the source data-set.
        """
        # See if the data-set already exists
        dataset_pk = get_existing_dataset(
            self.dataset_methods[0],
            self.ufdl_context,
            self.project_pk,
            self.dataset,
            self.version
        )

        # Dataset already exists
        if dataset_pk is not None:
            # Raise an error if this mode is selected
            if self.on_existing == "error":
                version_string = "" if self.version is None else f" (v{self.version})"
                raise Exception(f"Dataset named '{self.dataset}'{version_string} already exists (pk = {dataset_pk})")

            # If we are overwriting or creating split data-sets, the source is the existing dataset
            elif self.on_existing == "overwrite" or (self.is_splitting and self.new_dataset_per_split):
                return dataset_pk

            # Otherwise copy the existing data-set
            else:
                return self.dataset_methods[2](self.ufdl_context, dataset_pk)['pk']

        # Dataset doesn't already exist
        else:
            # If we're going to create a new dataset per split, don't need to create a source
            if self.is_splitting and self.new_dataset_per_split:
                return None

            # Otherwise create the source dataset
            else:
                return self._create_new_dataset(self.dataset)

    def _init_target_dataset(self) -> int:
        """
        Initialises the target data-set for each split.

        :return:    The target data-set's primary key.
        """
        if self.is_splitting and self.new_dataset_per_split:
            split_name = f"{self.dataset}-{self.split_label}"
            if self.source_dataset is None:
                return self._create_new_dataset(split_name)
            else:
                return self.dataset_methods[2](self.ufdl_context, self.source_dataset, split_name)['pk']
        else:
            return self.source_dataset

    def _create_new_dataset(self, name: str) -> int:
        """
        Creates a new dataset in the given project.

        :param name:        The name to give the dataset.
        :param project_pk:  The project in which to create the new dataset.
        :return:            The primary key of the new dataset.
        """
        # See if there is an existing dataset
        if get_existing_dataset(self.dataset_methods[0], self.ufdl_context, self.project_pk, name) is not None:
            raise Exception(f"A dataset named '{name}' already exists")

        return self.dataset_methods[1](
            self.ufdl_context,
            name,
            self.project_pk,
            self.licence_pk,
            self.description if self.description is not None else "",
            self.is_public,
            ",".join(self.tags)
        )['pk']

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

from abc import abstractmethod
from io import BytesIO
from typing import List, TypeVar

from ufdl.pythonclient.functional.core import dataset

from wai.annotations.core.component import SourceComponent
from wai.annotations.core.stream import ThenFunction, DoneFunction

from wai.common.cli.options import TypedOption

from ..util import get_existing_dataset
from .util import UFDLProjectSpecificMixin

ExternalFormat = TypeVar("ExternalFormat")


class UFDLReader(UFDLProjectSpecificMixin, SourceComponent[ExternalFormat]):
    """
    Base class for readers which read datasets from a UFDL server.
    """
    datasets: List[str] = TypedOption(
        "--datasets",
        type=str,
        nargs="+",
        required=True,
        help="the list of datasets to convert (omit version for latest)",
        metavar="NAME[==VERSION]"
    )

    def produce(
            self,
            then: ThenFunction[ExternalFormat],
            done: DoneFunction
    ):
        for ds in self.datasets:
            # Parse the name/version from the string
            if "==" in ds:
                name, version = ds.split("==", 1)
                version = int(version)
            else:
                name, version = ds, None

            # Get the dataset's pk
            pk = get_existing_dataset(dataset.list, self.ufdl_context, self.project_pk, name, version)

            # Get the list of files in the dataset
            files = dataset.retrieve(self.ufdl_context, pk)["files"]

            for file in files:
                # Download the file data
                file_data = self.download_file_data(pk, file)

                self.read_annotations(pk, file, file_data, then)

        done()

    @abstractmethod
    def read_annotations(
            self,
            pk: int,
            filename: str,
            file_data: bytes,
            then: ThenFunction[ExternalFormat]
    ):
        """
        Finalises the read by downloading the annotations for the file and
        returning the instance.

        :param pk:          The primary key of the dataset being accessed.
        :param filename:    The filename of the file being converted.
        :param file_data:   The binary file data of the file being converted.
        :param then:        The function to forward the element.
        :return:            An iterator of annotated instances.
        """
        pass

    def download_file_data(self, pk: int, filename: str) -> bytes:
        """
        Downloads the file-data for a particular file.

        :param pk:          The primary key of the dataset containing the file.
        :param filename:    The filename of the file in the dataset.
        :return:            The binary contents of the file.
        """
        # Create a binary buffer for accumulating the contents
        buffer = BytesIO()

        # Stream the contents from the server into the buffer
        for chunk in dataset.get_file(self.ufdl_context, pk, filename):
            buffer.write(chunk)

        # Return the contents of the buffer
        buffer.seek(0)
        return buffer.read()

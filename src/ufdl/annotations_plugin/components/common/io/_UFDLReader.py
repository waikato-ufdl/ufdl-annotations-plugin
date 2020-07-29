from abc import abstractmethod
from io import BytesIO
from typing import Iterator, List, TypeVar

from ufdl.pythonclient.functional.core import dataset

from wai.annotations.core.component import Reader

from wai.common.cli.options import TypedOption

from .._UFDLContextOptionsMixin import UFDLContextOptionsMixin

ExternalFormat = TypeVar("ExternalFormat")


class UFDLReader(Reader[ExternalFormat], UFDLContextOptionsMixin):
    """
    Base class for readers which read datasets from a UFDL server.
    """
    pks: List[int] = TypedOption(
        "--pks",
        type=int,
        nargs="+",
        required=True,
        help="the list of primary keys of the datasets to convert",
        metavar="PK"
    )

    def get_annotation_files(self) -> Iterator[str]:

        for pk in self.pks:
            # Get the list of files in the dataset
            files = dataset.retrieve(self._context, pk)["files"]

            for file in files:
                yield f"{pk}:{file}"

    def get_negative_files(self) -> Iterator[str]:
        # No way to specify negative files
        return iter(tuple())

    def read_annotation_file(self, filename: str) -> Iterator[ExternalFormat]:
        # Extract the pk and filename
        pk, filename = filename.split(":", 1)
        pk = int(pk)

        # Download the file data
        file_data = self.download_file_data(pk, filename)

        return self.read_annotations(pk, filename, file_data)

    @abstractmethod
    def read_annotations(self, pk: int, filename: str, file_data: bytes) -> Iterator[ExternalFormat]:
        """
        Finalises the read by downloading the annotations for the file and
        returning the instance.

        :param pk:          The primary key of the dataset being accessed.
        :param filename:    The filename of the file being converted.
        :param file_data:   The binary file data of the file being converted.
        :return:            An iterator of annotated instances.
        """
        pass

    def read_negative_file(self, filename: str) -> Iterator[ExternalFormat]:
        # No way to specify negative files
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
        for chunk in dataset.get_file(self._context, pk, filename):
            buffer.write(chunk)

        # Return the contents of the buffer
        buffer.seek(0)
        return buffer.read()

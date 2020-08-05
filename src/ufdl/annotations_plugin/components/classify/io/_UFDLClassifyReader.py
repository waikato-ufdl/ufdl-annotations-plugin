from typing import Iterator

from ufdl.json.image_classification import CategoriesFile

from ufdl.pythonclient.functional.image_classification import dataset

from wai.annotations.domain.image import ImageInfo

from ...common.io import UFDLReader
from .._format import UFDLClassifyExternalFormat


class UFDLClassifyReader(UFDLReader[UFDLClassifyExternalFormat]):
    """
    Reader which reads image classification annotations from a UFDL server.
    """
    def read_annotations(self, pk: int, filename: str, file_data: bytes) -> Iterator[UFDLClassifyExternalFormat]:
        # Download the annotations
        annotations = CategoriesFile.from_raw_json(dataset.get_categories(self.ufdl_context, pk))

        yield UFDLClassifyExternalFormat(
            ImageInfo.from_file_data(filename, file_data),
            annotations[filename]
        )

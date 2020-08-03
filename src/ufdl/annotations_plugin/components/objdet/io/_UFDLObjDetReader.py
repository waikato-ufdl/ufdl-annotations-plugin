from typing import Iterator

from ufdl.json.object_detection import Annotation

from ufdl.pythonclient.functional.object_detection import dataset

from wai.annotations.domain.image import ImageInfo

from ...common.io import UFDLReader
from .._format import UFDLObjDetExternalFormat


class UFDLObjDetReader(UFDLReader[UFDLObjDetExternalFormat]):
    """
    Reader which reads object-detection annotations from a UFDL server.
    """
    def read_annotations(self, pk: int, filename: str, file_data: bytes) -> Iterator[UFDLObjDetExternalFormat]:
        # Download the annotations
        annotations = dataset.get_annotations_for_image(self.ufdl_context, pk, filename)

        yield UFDLObjDetExternalFormat(
            ImageInfo.from_file_data(filename, file_data),
            [Annotation.from_raw_json(ann_string) for ann_string in annotations]
        )

from typing import Iterator

from ufdl.annotation_utils.object_detection import annotation_from_located_object

from wai.annotations.core.component import OutputConverter
from wai.annotations.domain.image.object_detection import ObjectDetectionInstance

from .._format import UFDLObjDetExternalFormat


class ToUFDLObjDet(OutputConverter[ObjectDetectionInstance, UFDLObjDetExternalFormat]):
    """
    Converts object-detection instances in their internal format into the type
    expected by a UFDL server.
    """
    def convert(self, instance: ObjectDetectionInstance) -> Iterator[UFDLObjDetExternalFormat]:
        yield UFDLObjDetExternalFormat(
            instance.file_info,
            list(map(annotation_from_located_object, instance.annotations))
        )

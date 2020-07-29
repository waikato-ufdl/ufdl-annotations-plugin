from typing import Iterator

from ufdl.annotation_utils import located_object_from_annotation

from wai.annotations.core.component import InputConverter
from wai.annotations.domain.image.object_detection import ObjectDetectionInstance

from wai.common.adams.imaging.locateobjects import LocatedObjects

from .._format import UFDLObjDetExternalFormat


class FromUFDLObjDet(InputConverter[UFDLObjDetExternalFormat, ObjectDetectionInstance]):
    """
    Converts an image and its annotations from the format received from the UFDL
    server into the internal format for the image object-detection domain in
    wai.annotations.
    """
    def convert(self, instance: UFDLObjDetExternalFormat) -> Iterator[ObjectDetectionInstance]:
        yield ObjectDetectionInstance(
            instance.image,
            LocatedObjects(map(located_object_from_annotation, instance.annotations))
        )

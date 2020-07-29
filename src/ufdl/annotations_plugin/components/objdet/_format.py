"""
Defines the external format for UFDL object-detection instances.
"""
from typing import List

from ufdl.json.object_detection import Annotation

from wai.annotations.domain.image import ImageInfo


class UFDLObjDetExternalFormat:
    """
    Basic object-detection format which holds an image and its
    annotations as received from a UFDL server.
    """
    def __init__(self, image: ImageInfo, annotations: List[Annotation]):
        self._image: ImageInfo = image
        self._annotations: List[Annotation] = annotations

    @property
    def image(self) -> ImageInfo:
        return self._image

    @property
    def annotations(self) -> List[Annotation]:
        return self._annotations

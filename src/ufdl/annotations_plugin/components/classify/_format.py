"""
Defines the external format for UFDL object-detection instances.
"""
from typing import List

from wai.annotations.domain.image import ImageInfo


class UFDLClassifyExternalFormat:
    """
    Basic image-classification format which holds an image and its
    categories as received from a UFDL server.
    """
    def __init__(self, image: ImageInfo, annotations: List[str]):
        self._image: ImageInfo = image
        self._annotations: List[str] = annotations

    @property
    def image(self) -> ImageInfo:
        return self._image

    @property
    def annotations(self) -> List[str]:
        return self._annotations

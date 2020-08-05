from typing import Iterator

from wai.annotations.core.component import InputConverter
from wai.annotations.domain.image.classification import ImageClassificationInstance

from .._format import UFDLClassifyExternalFormat


class FromUFDLClassify(InputConverter[UFDLClassifyExternalFormat, ImageClassificationInstance]):
    """
    Converts an image and its annotations from the format received from the UFDL
    server into the internal format for the image classification domain in
    wai.annotations.
    """
    def convert(self, instance: UFDLClassifyExternalFormat) -> Iterator[ImageClassificationInstance]:
        yield ImageClassificationInstance(
            instance.image,
            instance.annotations[0] if len(instance.annotations) > 0 else ""
        )

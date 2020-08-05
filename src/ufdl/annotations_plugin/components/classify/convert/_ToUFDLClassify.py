from typing import Iterator

from wai.annotations.core.component import OutputConverter
from wai.annotations.domain.image.classification import ImageClassificationInstance

from .._format import UFDLClassifyExternalFormat


class ToUFDLClassify(OutputConverter[ImageClassificationInstance, UFDLClassifyExternalFormat]):
    """
    Converts image classification instances in their internal format into the type
    expected by a UFDL server.
    """
    def convert(self, instance: ImageClassificationInstance) -> Iterator[UFDLClassifyExternalFormat]:
        yield UFDLClassifyExternalFormat(
            instance.file_info,
            [instance.annotations] if instance.annotations != "" else []
        )

from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SourceStageSpecifier


class UFDLImageObjectDetectionSourceStageSpecifier(SourceStageSpecifier):
    """
    Specifies an object-detection input format that uses an UFDL server as its source.
    """
    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLImageObjectDetectionReader
        return UFDLImageObjectDetectionReader,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.object_detection import ImageObjectDetectionDomainSpecifier
        return ImageObjectDetectionDomainSpecifier

    @classmethod
    def description(cls) -> str:
        return "An input format which reads object-detection instances from a UFDL data-set"

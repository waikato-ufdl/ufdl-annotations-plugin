from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SinkStageSpecifier


class UFDLImageObjectDetectionSinkStageSpecifier(SinkStageSpecifier):
    """
    Specifies an object-detection output format that uses an UFDL server as its target.
    """
    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLImageObjectDetectionWriter
        return UFDLImageObjectDetectionWriter,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.object_detection import ImageObjectDetectionDomainSpecifier
        return ImageObjectDetectionDomainSpecifier

    @classmethod
    def description(cls) -> str:
        return "An output format which writes object-detection instances to a UFDL data-set"

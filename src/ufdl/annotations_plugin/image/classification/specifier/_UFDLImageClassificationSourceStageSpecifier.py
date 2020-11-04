from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SourceStageSpecifier


class UFDLImageClassificationSourceStageSpecifier(SourceStageSpecifier):
    """
    Specifies an image-classification input format that uses an UFDL server as its source.
    """
    @classmethod
    def description(cls) -> str:
        return "An input format which reads image-classification instances from a UFDL data-set"

    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLImageClassificationReader
        return UFDLImageClassificationReader,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.classification import ImageClassificationDomainSpecifier
        return ImageClassificationDomainSpecifier

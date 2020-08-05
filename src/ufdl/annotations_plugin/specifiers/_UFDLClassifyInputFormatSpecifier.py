from typing import Type

from wai.annotations.core.component import InputConverter, Reader
from wai.annotations.core.specifier import InputFormatSpecifier, DomainSpecifier


class UFDLClassifyInputFormatSpecifier(InputFormatSpecifier):
    """
    Specifies an image-classification input format that uses an UFDL server as its source.
    """
    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.classification import ImageClassificationDomainSpecifier
        return ImageClassificationDomainSpecifier

    @classmethod
    def reader(cls) -> Type[Reader]:
        from ..components.classify.io import UFDLClassifyReader
        return UFDLClassifyReader

    @classmethod
    def input_converter(cls) -> Type[InputConverter]:
        from ..components.classify.convert import FromUFDLClassify
        return FromUFDLClassify

    @classmethod
    def description(cls) -> str:
        return "An input format which reads image-classification instances from a UFDL data-set"

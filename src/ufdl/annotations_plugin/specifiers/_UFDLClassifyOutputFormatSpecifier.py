from typing import Type

from wai.annotations.core.component import OutputConverter, Writer
from wai.annotations.core.specifier import OutputFormatSpecifier, DomainSpecifier


class UFDLClassifyOutputFormatSpecifier(OutputFormatSpecifier):
    """
    Specifies an image classification output format that uses an UFDL server as its target.
    """
    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.classification import ImageClassificationDomainSpecifier
        return ImageClassificationDomainSpecifier

    @classmethod
    def writer(cls) -> Type[Writer]:
        from ..components.classify.io import UFDLClassifyWriter
        return UFDLClassifyWriter

    @classmethod
    def output_converter(cls) -> Type[OutputConverter]:
        from ..components.classify.convert import ToUFDLClassify
        return ToUFDLClassify

    @classmethod
    def description(cls) -> str:
        return "An output format which writes image classification instances to a UFDL data-set"

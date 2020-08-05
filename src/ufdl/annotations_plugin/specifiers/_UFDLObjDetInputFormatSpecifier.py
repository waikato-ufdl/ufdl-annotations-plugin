from typing import Type

from wai.annotations.core.component import InputConverter, Reader
from wai.annotations.core.specifier import InputFormatSpecifier, DomainSpecifier


class UFDLObjDetInputFormatSpecifier(InputFormatSpecifier):
    """
    Specifies an object-detection input format that uses an UFDL server as its source.
    """
    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.object_detection import ImageObjectDetectionDomainSpecifier
        return ImageObjectDetectionDomainSpecifier

    @classmethod
    def reader(cls) -> Type[Reader]:
        from ..components.objdet.io import UFDLObjDetReader
        return UFDLObjDetReader

    @classmethod
    def input_converter(cls) -> Type[InputConverter]:
        from ..components.objdet.convert import FromUFDLObjDet
        return FromUFDLObjDet

    @classmethod
    def description(cls) -> str:
        return "An input format which reads object-detection instances from a UFDL data-set"

from typing import Type

from wai.annotations.core.component import OutputConverter, Writer
from wai.annotations.core.specifier import OutputFormatSpecifier, DomainSpecifier


class UFDLObjDetOutputFormatSpecifier(OutputFormatSpecifier):
    """
    Specifies an output format that uses an UFDL server as its target.
    """
    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.image.object_detection import ImageObjectDetectionDomainSpecifier
        return ImageObjectDetectionDomainSpecifier

    @classmethod
    def writer(cls) -> Type[Writer]:
        from ..components.objdet.io import UFDLObjDetWriter
        return UFDLObjDetWriter

    @classmethod
    def output_converter(cls) -> Type[OutputConverter]:
        from ..components.objdet.convert import ToUFDLObjDet
        return ToUFDLObjDet

    @classmethod
    def description(cls) -> str:
        return "An output format which writes object-detection instances to a UFDL data-set"

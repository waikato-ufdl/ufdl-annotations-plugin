from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SinkStageSpecifier


class UFDLSpectrumClassificationSinkStageSpecifier(SinkStageSpecifier):
    """
    Specifies a spectrum classification output format that uses an UFDL server as its target.
    """
    @classmethod
    def description(cls) -> str:
        return "An output format which writes spectrum classification instances to a UFDL data-set"

    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLSpectrumClassificationWriter
        return UFDLSpectrumClassificationWriter,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.spectra.classification import SpectrumClassificationDomainSpecifier
        return SpectrumClassificationDomainSpecifier

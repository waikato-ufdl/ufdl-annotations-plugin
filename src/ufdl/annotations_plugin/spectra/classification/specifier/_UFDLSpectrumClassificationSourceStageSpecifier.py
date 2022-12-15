from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SourceStageSpecifier


class UFDLSpectrumClassificationSourceStageSpecifier(SourceStageSpecifier):
    """
    Specifies a spectrum-classification input format that uses an UFDL server as its source.
    """
    @classmethod
    def description(cls) -> str:
        return "An input format which reads spectrum-classification instances from a UFDL data-set"

    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLSpectrumClassificationReader
        return UFDLSpectrumClassificationReader,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.spectra.classification import SpectrumClassificationDomainSpecifier
        return SpectrumClassificationDomainSpecifier

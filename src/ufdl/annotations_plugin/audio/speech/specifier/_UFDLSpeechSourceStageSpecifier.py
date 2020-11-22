from typing import Type, Tuple

from wai.annotations.core.component import Component
from wai.annotations.core.domain import DomainSpecifier
from wai.annotations.core.specifier import SourceStageSpecifier


class UFDLSpeechSourceStageSpecifier(SourceStageSpecifier):
    """
    Specifies a speech input format that uses an UFDL server as its source.
    """
    @classmethod
    def description(cls) -> str:
        return "An input format which reads speech instances from a UFDL data-set"

    @classmethod
    def components(cls) -> Tuple[Type[Component], ...]:
        from ..component import UFDLSpeechReader
        return UFDLSpeechReader,

    @classmethod
    def domain(cls) -> Type[DomainSpecifier]:
        from wai.annotations.domain.audio.speech import SpeechDomainSpecifier
        return SpeechDomainSpecifier

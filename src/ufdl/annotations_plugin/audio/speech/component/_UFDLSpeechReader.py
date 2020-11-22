from typing import Dict

from ufdl.json.speech import TranscriptionsFile

from ufdl.pythonclient.functional.speech import dataset

from wai.annotations.core.stream import ThenFunction
from wai.annotations.core.stream.util import ProcessState
from wai.annotations.domain.audio import Audio
from wai.annotations.domain.audio.speech import SpeechInstance, Transcription

from ....common.component import UFDLReader


class UFDLSpeechReader(UFDLReader[SpeechInstance]):
    """
    Reader which reads speech annotations from a UFDL server.
    """
    # Caches the transcription file for each dataset so it need only be retrieved once
    transcription_cache: Dict[int, TranscriptionsFile] = ProcessState(lambda self: {})

    def read_annotations(
            self,
            pk: int,
            filename: str,
            file_data: bytes,
            then: ThenFunction[SpeechInstance]
    ):
        # Get the cache
        transcription_cache = self.transcription_cache

        # Download the transcriptions for this data-set if we haven't already
        if pk not in transcription_cache:
            transcription_cache[pk] = TranscriptionsFile.from_raw_json(dataset.get_transcriptions(self.ufdl_context, pk))

        # Get the transcription file for this dataset from the cache
        transcription_file = transcription_cache[pk]

        # Select the first transcription for the file, if any
        transcription = None
        if filename in transcription_file:
            transcription = transcription_file[filename].transcription

        then(
            SpeechInstance(
                Audio.from_file_data(filename, file_data),
                Transcription(transcription) if transcription is not None else None
            )
        )

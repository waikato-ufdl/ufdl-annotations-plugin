import os
import re
import tempfile
from abc import ABC, abstractmethod
from fnmatch import fnmatchcase
from fractions import Fraction
from typing import Dict, Iterator, List, Optional, Tuple

from moviepy.video.io.VideoFileClip import VideoFileClip
from ufdl.json.object_detection import (
    Annotation,
    ImageAnnotation,
    Image as JSONImage,
    Video as JSONVideo
)

from ufdl.pythonclient.functional.object_detection import dataset

from wai.annotations.core.stream import ThenFunction
from wai.annotations.domain.image import Image, ImageFormat
from wai.annotations.domain.image.object_detection import ImageObjectDetectionInstance
from wai.annotations.domain.image.object_detection.util import set_object_label, set_object_prefix

from wai.common.adams.imaging.locateobjects import LocatedObjects, LocatedObject
from wai.common.cli import CLIRepresentable
from wai.common.cli.options import TypedOption

from wai.json.object import Absent
from wai.json.raw import RawJSONArray, RawJSONObject

from ....common.component import UFDLReader


class UnlabelledExtractionSpecHolder(CLIRepresentable):
    def cli_repr(self) -> str:
        return self._repr

    @classmethod
    def from_cli_repr(cls, cli_string: str) -> 'UnlabelledExtractionSpecHolder':
        return UnlabelledExtractionSpecHolder(cli_string)

    def __init__(self, repr: str):
        self._repr = repr
        self._spec = UnlabelledExtractionSpec.from_string(repr)

    @property
    def spec(self):
        return self._spec


class UnlabelledExtractionSpec(ABC):
    # Fraction parsing is implemented by the Fraction constructor, hence the .* matchers
    STRING_SPEC_REGEX = re.compile(
        r"""
        ^                                               # Regex start
        ((?P<filename_glob>.*)@)?                         # Optional glob pattern for matching filenames
        (
            (every:(?P<step>-?[^+-]*)(?P<offset>[+-].*)?) # An every step specifier with optional offset
            |                                           # or,
            (?P<times>[^,]*(,[^,]*)*)                   # a comma separated list of specific times
        )
        $                                               # Regex end
        """,
        flags=re.VERBOSE
    )

    def __init__(
            self,
            filename_glob_pattern: str
    ):
        self._filename_glob_pattern = filename_glob_pattern

    def matches_filename(self, filename: str) -> bool:
        return fnmatchcase(filename, self._filename_glob_pattern)

    @staticmethod
    def from_string(string: str) -> 'UnlabelledExtractionSpec':
        match = UnlabelledExtractionSpec.STRING_SPEC_REGEX.match(string)
        glob = match.group('filename_glob')
        if glob is None:
            glob = "*"
        step = match.group('step')
        if step is not None:
            step = Fraction(step)
            offset = match.group('offset')
            if offset is None:
                offset = Fraction()
            else:
                offset = Fraction(offset)
            return EveryUnlabelledExtractionSpec(glob, step, offset)
        else:
            times = match.group('times').split(',')
            return SpecificUnlabelledExtractionSpec(glob, *map(Fraction, times))

    @abstractmethod
    def get_times(self, length: float) -> Iterator[float]:
        raise NotImplementedError(self.get_times.__qualname__)


class EveryUnlabelledExtractionSpec(UnlabelledExtractionSpec):
    def __init__(
            self,
            filename_glob_pattern: str,
            step: Fraction,
            offset: Fraction
    ):
        super().__init__(filename_glob_pattern)
        if step == Fraction():
            raise ValueError(f"Step can't be zero")
        self._step = step
        self._offset = offset

    def get_times(self, length: float) -> Iterator[float]:
        step = self._step
        start = self._offset
        if step < 0:
            if start > 0:
                start %= step
            start += length
        else:
            if start < 0:
                start %= 0

        class It(Iterator[float]):
            def __next__(self) -> float:
                nonlocal step, start, length
                result = start
                if result > length or result < 0:
                    raise StopIteration
                start += step
                return float(result)

        return It()


class SpecificUnlabelledExtractionSpec(UnlabelledExtractionSpec):
    def __init__(
            self,
            filename_glob_pattern: str,
            *times: Fraction
    ):
        super().__init__(filename_glob_pattern)
        self._times = list(times)

    def get_times(self, length: float) -> Iterator[float]:
        yield from (
            float(time)
            for time in self._times
            if 0 <= time <= length
        )


class UFDLImageObjectDetectionReader(UFDLReader[ImageObjectDetectionInstance]):
    """
    Reader which reads object-detection annotations from a UFDL server.
    """
    # Specification of unlabelled frames to extract from videos
    extract_unlabelled: Optional[List[UnlabelledExtractionSpecHolder]] = TypedOption(
        "--unlabelled",
        type=UnlabelledExtractionSpecHolder,
        nargs="+",
        required=False,
        help="the list of datasets to convert (omit version for latest)",
        metavar="[GLOB@](every:STEP[+/-OFFSET] | TIME[,TIME]*"
    )

    def read_annotations(
            self,
            pk: int,
            filename: str,
            file_data: bytes,
            then: ThenFunction[ImageObjectDetectionInstance]
    ):
        for image, annotations in self.get_instances(pk, filename, file_data):
            then(
                ImageObjectDetectionInstance(
                    image,
                    LocatedObjects(
                        map(self.located_object_from_annotation, annotations)
                    )
                )
            )

    @staticmethod
    def located_object_from_annotation(annotation: Annotation) -> LocatedObject:
        """
        Creates a located object from an annotation.

        :param annotation:  The annotation.
        :return:            The located object.
        """
        # Create the basic located object
        located_object = LocatedObject(annotation.x, annotation.y, annotation.width, annotation.height)

        # Add the polygon if present
        if annotation.polygon is not Absent:
            located_object.set_polygon(annotation.polygon.to_geometric_polygon())

        # Set the label
        set_object_label(located_object, annotation.label)

        # Set the prefix if present
        if annotation.prefix is not Absent:
            set_object_prefix(located_object, annotation.prefix)

        return located_object

    def get_instances(
            self,
            pk: int,
            filename: str,
            file_data: bytes
    ) -> Iterator[Tuple[Image, List[ImageAnnotation]]]:
        # Get the file type
        file_type = dataset.get_file_type(self.ufdl_context, pk, filename)

        # If the file-type hasn't been set, skip this file
        if file_type is None:
            return

        # Get the annotations
        annotations = dataset.get_annotations_for_file(self.ufdl_context, pk, filename)

        if file_type.get('length', None) is None:
            yield self.get_image_instance(filename, file_data, file_type, annotations)
        else:
            yield from self.get_video_frame_instances(filename, file_data, file_type, annotations)

    @staticmethod
    def get_image_instance(
            filename: str,
            file_data: bytes,
            file_type: RawJSONObject,
            annotations: RawJSONArray
    ) -> Tuple[Image, List[ImageAnnotation]]:
        # Parse the raw JSON
        json_image: JSONImage = JSONImage.from_raw_json(
            {
                **file_type,
                'annotations': annotations
            }
        )

        # Extract the format and dimensions
        format = (
            ImageFormat.for_extension(json_image.format) if json_image.format is not Absent
            else None
        )
        dimensions = (
            json_image.dimensions if json_image.dimensions is not Absent
            else None
        )

        return (
            Image(
                filename,
                file_data,
                format,
                dimensions
            ),
            json_image.annotations
        )

    def get_video_frame_instances(
            self,
            filename: str,
            file_data: bytes,
            file_type: RawJSONObject,
            annotations: RawJSONArray
    ) -> Iterator[Tuple[Image, List[ImageAnnotation]]]:
        # Parse the raw JSON
        json_video: JSONVideo = JSONVideo.from_raw_json(
            {
                **file_type,
                'annotations': annotations
            }
        )

        # Extract the format and dimensions
        format = ImageFormat.for_extension("jpg")  # We always extract frames as JPGs
        dimensions = (
            json_video.dimensions if json_video.dimensions is not Absent
            else None
        )

        # Get the unlabelled extraction specifiers that correspond to this video
        unlabelled_extractors = [
            extractor
            for extractor in self.extract_unlabelled
            if extractor.spec.matches_filename(filename)
        ] if self.extract_unlabelled is not None else []

        # Create a set of timestamps to extract (of unlabelled frames)
        extraction_times = set(
            time
            for extractor in unlabelled_extractors
            for time in extractor.spec.get_times(json_video.length)
        )

        # Add the labelled frame-times as well
        extraction_times.update(
            annotation.time
            for annotation in json_video.annotations
        )

        # If there are no frames to extract, skip this video
        if len(extraction_times) == 0:
            return

        # Group the annotations by frame-time (as image-annotations)
        frame_annotations: Dict[float, List[ImageAnnotation]] = {}
        for annotation in json_video.annotations:
            frame_annotation_list = frame_annotations.get(annotation.time)
            if frame_annotation_list is None:
                frame_annotation_list = []
                frame_annotations[annotation.time] = frame_annotation_list
            frame_annotation_list.append(annotation.to_image_annotation())

        # We need to write the video data to disk so that FFMPEG can read it,
        # so do so in a temporary file
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a name for the temporary copy of the video data
            video_filename = os.path.join(tmp_dir, "video")

            # Copy the video data to a temporary file
            with open(video_filename, "wb") as tmp_video_file:
                tmp_video_file.write(file_data)

            # Open the video with FFMPEG
            with VideoFileClip(video_filename, audio=False) as video_clip:
                # Create images
                for frame_time in extraction_times:
                    # Create an augmented filename for this frame of the video
                    augmented_filename = f"{filename}@|frametime={frame_time}|.jpg"

                    # Create a temporary filename to store the frame image under
                    tmp_image_filename = os.path.join(tmp_dir, "image.jpg")

                    # Save the frame as a temporary JPEG
                    video_clip.save_frame(tmp_image_filename, frame_time, False)

                    # Read the image data back in to memory
                    with open(tmp_image_filename, "rb") as tmp_image_file:
                        frame_data = tmp_image_file.read()

                    # Create an image descriptor for the frame
                    image = Image(
                        augmented_filename,
                        frame_data,
                        format,
                        dimensions
                    )

                    yield image, frame_annotations.get(frame_time, [])

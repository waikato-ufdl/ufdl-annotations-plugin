import os
import tempfile
from typing import Iterator, List, Tuple

from moviepy.video.io.VideoFileClip import VideoFileClip
from ufdl.json.object_detection import (
    Annotation,
    ImageAnnotation,
    VideoAnnotation,
    Image as JSONImage,
    Video as JSONVideo
)

from ufdl.pythonclient.functional.object_detection import dataset

from wai.annotations.core.stream import ThenFunction
from wai.annotations.domain.image import Image, ImageFormat
from wai.annotations.domain.image.object_detection import ImageObjectDetectionInstance
from wai.annotations.domain.image.object_detection.util import set_object_label, set_object_prefix

from wai.common.adams.imaging.locateobjects import LocatedObjects, LocatedObject

from wai.json.object import Absent

from ....common.component import UFDLReader


class UFDLImageObjectDetectionReader(UFDLReader[ImageObjectDetectionInstance]):
    """
    Reader which reads object-detection annotations from a UFDL server.
    """
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

        annotations = dataset.get_annotations_for_file(self.ufdl_context, pk, filename)

        if file_type.get('length', None) is None:
            json_image: JSONImage = JSONImage.from_raw_json(
                {
                    **file_type,
                    'annotations': annotations
                }
            )
            format = (
                ImageFormat.for_extension(json_image.format) if json_image.format is not Absent
                else None
            )
            dimensions = (
                json_image.dimensions if json_image.dimensions is not Absent
                else None
            )

            yield (
                Image(
                    filename,
                    file_data,
                    format,
                    dimensions
                ),
                json_image.annotations
            )
        else:
            json_video: JSONVideo = JSONVideo.from_raw_json(
                {
                    **file_type,
                    'annotations': annotations
                }
            )
            format = ImageFormat.for_extension("jpg")
            dimensions = (
                json_video.dimensions if json_video.dimensions is not Absent
                else None
            )

            # Get the annotations and sort them by frame-time
            video_annotations: List[VideoAnnotation] = list(json_video.annotations)
            video_annotations.sort(key=lambda video_annotation: video_annotation.time)

            # If there are no annotated frames, skip this video
            if len(video_annotations) == 0:
                return

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
                    while len(video_annotations) > 0:
                        # Get the frame-time we are creating an image for
                        frame_time = video_annotations[0].time

                        # Convert the video annotations for this frame to image annotations
                        image_annotations: List[ImageAnnotation] = []
                        while len(video_annotations) > 0 and video_annotations[0].time == frame_time:
                            image_annotations.append(video_annotations.pop(0).to_image_annotation())

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

                        yield image, image_annotations

from ufdl.json.object_detection import Annotation

from ufdl.annotation_utils.object_detection import located_object_from_annotation

from ufdl.pythonclient.functional.object_detection import dataset

from wai.annotations.core.stream import ThenFunction
from wai.annotations.domain.image import Image
from wai.annotations.domain.image.object_detection import ImageObjectDetectionInstance

from wai.common.adams.imaging.locateobjects import LocatedObjects

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
        # Download the annotations
        annotations = dataset.get_annotations_for_image(self.ufdl_context, pk, filename)

        then(
            ImageObjectDetectionInstance(
                Image.from_file_data(filename, file_data),
                LocatedObjects(
                    located_object_from_annotation(Annotation.from_raw_json(ann_string))
                    for ann_string in annotations
                )
            )
        )

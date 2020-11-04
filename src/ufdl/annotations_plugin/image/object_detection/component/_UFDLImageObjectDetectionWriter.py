from typing import Optional, Dict, Set

from ufdl.annotation_utils.object_detection import annotation_from_located_object

from ufdl.pythonclient.functional.object_detection import dataset

from wai.annotations.core.stream.util import ProcessState
from wai.annotations.domain.image.object_detection import ImageObjectDetectionInstance

from ....common.component import UFDLWriter
from ....common.component.util import DatasetMethods


class UFDLImageObjectDetectionWriter(UFDLWriter[ImageObjectDetectionInstance]):
    """
    Writes instances to a data-set on a UFDL server.
    """
    existing_files_cache: Dict[int, Set[str]] = ProcessState(lambda self: {})

    def get_dataset_methods(self) -> DatasetMethods:
        return dataset.list, dataset.create, dataset.copy

    def write_to_dataset(
            self,
            element: ImageObjectDetectionInstance,
            dataset_pk: int,
            subfolder: Optional[str]
    ):
        # Get the existing files cache
        existing_files_cache = self.existing_files_cache

        if dataset_pk not in existing_files_cache:
            existing_files_cache[dataset_pk] = set(dataset.retrieve(self.ufdl_context, dataset_pk)['files'])

        # Format the file-name with the folder
        filename = element.data.filename if subfolder is None else f"{subfolder}/{element.data.filename}"

        # If this file already exists, delete it
        if filename in existing_files_cache[dataset_pk]:
            dataset.delete_file(self.ufdl_context, dataset_pk, filename)

        # Upload the file data
        dataset.add_file(self.ufdl_context, dataset_pk, filename, element.data.data)

        # Add the filename to the cache
        existing_files_cache[dataset_pk].add(filename)

        # Upload the annotations
        if element.annotations is not None:
            dataset.set_annotations_for_image(
                self.ufdl_context,
                dataset_pk,
                filename,
                list(map(annotation_from_located_object, element.annotations))
            )

from typing import Iterable, Optional, Tuple

from ufdl.pythonclient.functional.image_classification import dataset

from wai.annotations.core.instance import FileInfo

from ...common.io import UFDLWriter, DATASET_LIST_METHOD_TYPE, DATASET_CREATE_METHOD_TYPE, DATASET_COPY_METHOD_TYPE
from .._format import UFDLClassifyExternalFormat


class UFDLClassifyWriter(UFDLWriter[UFDLClassifyExternalFormat]):
    """
    Writes instances to a data-set on a UFDL server.
    """
    def get_dataset_methods(self) -> Tuple[DATASET_LIST_METHOD_TYPE, DATASET_CREATE_METHOD_TYPE, DATASET_COPY_METHOD_TYPE]:
        return dataset.list, dataset.create, dataset.copy

    def write_to_dataset(self, instances: Iterable[UFDLClassifyExternalFormat], dataset_pk: int, subfolder: Optional[str]):
        # Get the files already in the dataset
        files = set(dataset.retrieve(self.ufdl_context, dataset_pk)['files'])

        for instance in instances:
            # Format the file-name with the folder
            filename = instance.image.filename if subfolder is None else f"{subfolder}/{instance.image.filename}"

            # If this file already exists, delete it
            if filename in files:
                dataset.delete_file(self.ufdl_context, dataset_pk, filename)

            # Upload the file data
            dataset.add_file(self.ufdl_context, dataset_pk, filename, instance.image.data)

            # Upload the annotations
            dataset.add_categories(self.ufdl_context, dataset_pk, [filename], instance.annotations)

    def extract_file_info_from_external_format(self, instance: UFDLClassifyExternalFormat) -> FileInfo:
        return instance.image

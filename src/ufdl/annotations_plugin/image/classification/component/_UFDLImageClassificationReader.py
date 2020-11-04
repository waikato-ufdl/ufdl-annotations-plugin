from typing import Dict

from ufdl.json.image_classification import CategoriesFile

from ufdl.pythonclient.functional.image_classification import dataset

from wai.annotations.core.stream import ThenFunction
from wai.annotations.core.stream.util import ProcessState
from wai.annotations.domain.classification import Classification
from wai.annotations.domain.image import Image
from wai.annotations.domain.image.classification import ImageClassificationInstance

from ....common.component import UFDLReader


class UFDLImageClassificationReader(UFDLReader[ImageClassificationInstance]):
    """
    Reader which reads image classification annotations from a UFDL server.
    """
    # Caches the categories file for each dataset so it need only be retrieved once
    category_cache: Dict[int, CategoriesFile] = ProcessState(lambda self: {})

    def read_annotations(
            self,
            pk: int,
            filename: str,
            file_data: bytes,
            then: ThenFunction[ImageClassificationInstance]
    ):
        # Get the cache
        category_cache = self.category_cache

        # Download the categories for this data-set if we haven't already
        if pk not in category_cache:
            category_cache[pk] = CategoriesFile.from_raw_json(dataset.get_categories(self.ufdl_context, pk))

        # Get the category file for this dataset from the cache
        category_file = category_cache[pk]

        # Select the first category for the file, if any
        category = None
        if filename in category_file:
            categories = category_file[filename]
            if len(categories) > 0:
                category = categories[0]

        then(
            ImageClassificationInstance(
                Image.from_file_data(filename, file_data),
                Classification(category) if category is not None else None
            )
        )

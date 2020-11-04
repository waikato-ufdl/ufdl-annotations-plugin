from typing import Tuple

from ...util import DATASET_LIST_METHOD_TYPE, DATASET_CREATE_METHOD_TYPE, DATASET_COPY_METHOD_TYPE

# The Python-client functions required for writing to a UFDL dataset
DatasetMethods = Tuple[DATASET_LIST_METHOD_TYPE, DATASET_CREATE_METHOD_TYPE, DATASET_COPY_METHOD_TYPE]

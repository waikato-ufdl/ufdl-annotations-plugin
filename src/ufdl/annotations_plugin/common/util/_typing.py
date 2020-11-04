from typing import Callable, Optional

from ufdl.json.core.filter import FilterSpec

from ufdl.pythonclient import UFDLServerContext

from wai.json.object import OptionallyPresent
from wai.json.raw import RawJSONArray, RawJSONObject

# The types of the UFDL Python-client functions used by the components
DATASET_LIST_METHOD_TYPE = Callable[[UFDLServerContext, Optional[FilterSpec]], RawJSONArray]
DATASET_CREATE_METHOD_TYPE = Callable[[UFDLServerContext, str, int, int, str, bool, str], RawJSONObject]
DATASET_COPY_METHOD_TYPE = Callable[[UFDLServerContext, int, OptionallyPresent[str]], RawJSONObject]
DATASET_RETRIEVE_METHOD_TYPE = Callable[[UFDLServerContext, int], RawJSONObject]

from typing import Optional

from ufdl.json.core.filter import OrderBy, FilterSpec
from ufdl.json.core.filter.field import Exact

from ufdl.pythonclient import UFDLServerContext

from .._typing import DATASET_LIST_METHOD_TYPE


def get_existing_dataset(list_function: DATASET_LIST_METHOD_TYPE,
                         context: UFDLServerContext,
                         project_pk: int,
                         name: str,
                         version: Optional[int] = None) -> Optional[int]:
    """
    Gets the primary key of the specified dataset, if it exists,
    or None if it doesn't.

    :param name:        The name of the dataset to look for.
    :param project_pk:  The primary key of the project to look in.
    :return:            Dataset primary key or None.
    """
    # If no version is supplied, look for the dataset with the given name that
    # has the highest version.
    if version is None:
        datasets = list_function(context, FilterSpec(
            expressions=[
                Exact(field="name", value=name) &
                Exact(field="project", value=project_pk)
            ],
            order_by=[
                OrderBy(field="version", ascending=False)
            ]
        ))

    # Version is supplied, make sure it exists
    else:
        datasets = list_function(context, FilterSpec(
            expressions=[
                Exact(field="name", value=name) &
                Exact(field="version", value=version) &
                Exact(field="project", value=project_pk)
            ]
        ))

    return None if len(datasets) == 0 else datasets[0]['pk']

from enum import Enum
from typing import Sequence, Optional, Union


class QueryTypes(Enum):
    SELECT = 'SELECT'
    INSERT = 'INSERT'


def _append_fields(query: str, fields: Sequence[str]):
    return f'{query} {", ".join(fields)}'


def _append_filters(query: str, filters: Sequence[str]):
    return f'{query} WHERE {" AND ".join(map(lambda s: f"{s}=?", filters))}'


def _append_order_by(query: str, order_by: Sequence[str]):
    return f'{query} ORDER BY {", ".join(order_by)}'


def build_select(fields: Sequence[str], table: str = None, filters: Optional[Sequence[str]] = None,
                 order_by: Optional[Sequence[str]] = None):
    """
    build_select is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    if not fields:
        raise ValueError('No fields were passed into build_select.')
    query = _append_fields('SELECT ', fields)
    query = f'{query} FROM {table}'
    if filters:
        query = _append_filters(query, filters=filters)
    if order_by:
        query = _append_order_by(query, order_by=order_by)
    return query


def _append_value_fill_ins(query, count):
    return f'{query} {", ".join("?"*count)}'


def build_insert(fields: Sequence[str], table: str):
    """
    build_insert is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    if not fields:
        raise ValueError('No fields were passed into build_insert.')
    query = _append_fields(f'INSERT INTO {table} (', fields)
    query = _append_value_fill_ins(f'{query})', len(fields))
    return f'{query})'


def build_query(query_type: Union[str, QueryTypes], fields: Sequence[str], table: str,
                filters: Optional[Sequence[str]] = None, order_by: Optional[Sequence[str]] = None):
    """
    build_query is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    # Assert query_type is of type QueryTypes
    try:
        query_type = QueryTypes[query_type]
    except KeyError:
        query_type = QueryTypes(query_type)

    # run the appropriate build function
    if query_type is QueryTypes.SELECT:
        return build_select(fields, table, filters, order_by)
    elif query_type is QueryTypes.INSERT:
        return build_insert(fields, table)


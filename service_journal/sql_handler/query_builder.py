from enum import Enum
from typing import Sequence, Optional


class QueryTypes(Enum):
    SELECT = 'SELECT'
    INSERT = 'INSERT'


def _append_fields(query: str, fields: Sequence[str], err_fun: str = '_append_fields'):
    if not fields:
        raise ValueError(f'No fields were passed into {err_fun}.')
    return f'{query} {", ".join(fields)}'


def build_select(fields: Sequence[str], table: str = None, filters: Optional[Sequence[str]] = None):
    """
    build_select is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    query = _append_fields('SELECT ', fields, err_fun='build_select')
    query += f' FROM {table}'
    if filters:
        query += f' WHERE {" AND ".join(map(lambda s: f"{s}=?", filters))}'
    return query


def build_insert(fields: Sequence[str], table: str):
    """
    build_insert is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    query = _append_fields(f'INSERT INTO {table} (', fields, err_fun='build_insert')
    query += f') VALUES ({", ".join("?"*len(fields))})'
    return query


def build_query(query_type: QueryTypes, fields: Sequence[str], table: str, filters: Optional[Sequence[str]] = None):
    """
    build_query is to ONLY be used with trusted values. Either hardcoded values or trusted configuration values should
    be used for any of the parameters.
    """
    if query_type is QueryTypes.SELECT:
        return build_select(fields, table, filters)
    elif query_type is QueryTypes.INSERT:
        return build_insert(fields, table)

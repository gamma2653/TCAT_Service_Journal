from datetime import datetime
from typing import Tuple, List, Mapping, Union

from dbt_classes import Journal as DBT_Journal


class Journal:

    def __init__(self):
        self.root = dict()

    def __dict__(self):
        return self.root

    def add_entry(self, key: Tuple[str, str, str, str],
                  result: Mapping[str, Union[bool, List[int], List[Tuple[int, datetime, datetime]]]]) -> None:
        """
        Adds a single entry to the dictionary.

        key
            A tuple of (route, trip, block, date) to be used as a key for an entry.
        result
            A dictionary of results for a given key.

        """
        self.root[key] = result

    def get_entry(self, key: Tuple[str, str, str, str]):
        try:
            return self.root[key]
        except KeyError:
            return None

    def __setitem__(self, key: Tuple[str, str, str, str],
                    value: Mapping[str, Union[bool, List[int], List[Tuple[int, datetime, datetime]]]]) -> None:
        return self.add_entry(key, value)

    def __getitem__(self, key: Tuple[str, str, str, str]) -> \
            Mapping[str, Union[bool, List[int], List[Tuple[int, datetime, datetime]]]]:
        return self.get_entry(key)

    def __delitem__(self, key: Tuple[str, str, str, str]) -> None:
        try:
            del self.root[key]
        except KeyError:
            return

    # TODO: Implement
    def to_dbt(self):
        journal = DBT_Journal()
        for (route, trip, block, day), results in self.root:
            raise NotImplemented

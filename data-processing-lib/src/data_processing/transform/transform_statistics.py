from typing import Any


class TransformStatistics:
    """
    Basic statistics class collecting basic execution statistics.
    It can be extended for specific processors
    """

    def __init__(self):
        """
        Init - setting up variables. All the statistics is collected in the dictionary
        """
        self.stats = {}

    def add_stats(self, stats=dict[str, Any]) -> None:
        """
        Add statistics
        :param stats - dictionary creating new statistics
        :return: None
        """
        for key, val in stats.items():
            self.stats[key] = self.stats.get(key, 0) + val

    def get_execution_stats(self) -> dict[str, Any]:
        """
        Get execution statistics
        :return:
        """
        return self.stats

    def remove_stats(self, keys: list[str]) -> None:
        """
        Delete statistics, corresponding to the set of keys
        :param keys: list of keys to delete
        :return: None
        """
        for key in keys:
            self.stats.pop(key, None)

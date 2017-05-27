import json
import itertools

from collections import defaultdict
from JsonTools.node import Node
from JsonTools.constants import *


class NodeMaster(Node):

    def __init__(self, _data, _parent=None, _type="master"):
        super().__init__(_parent, _type, self)
        self._data = self.load_data(_data)
        self._master_keys = defaultdict(list)

        node_type = type(self._data)

        if node_type in (list, dict):
            Node(self, "main", self).add_node(self._data, None)

    @property
    def master_keys(self):
        return self._master_keys

    def get_data(self, by_key=None, get_values=False, allow_different_nodes=True, path_filter=None):

        if by_key:
            nodes = set.intersection(
                *map(
                    set, [self.master_keys[key] for key in by_key]
                )
            )

            # check if data is allowed to be in different nodes
            if allow_different_nodes:
                pass
            elif len(nodes) > 1:
                print("Warning, different nodes") #  replace by logger
                return False

            for node in nodes:
                data_objects = self.get_node_data(node, path_filter)

                if get_values:
                    for data_object in data_objects:
                        yield [data_object[key] for key in by_key]

                yield from data_objects

    def get_nodes(self, by_key=None):

        if by_key:
            nodes = set.intersection(
                *map(
                    set, [self.master_keys[key] for key in by_key]
                )
            )
            for node in nodes:
                yield node

    def get_node_data(self, node, path_filter=None):
        paths = self.get_node_paths(node)
        for path in paths:
            if path_filter and path_filter(path):
                continue

            yield self.get_data_by_path(path)

    def get_node_paths(self, node):
        values = []
        while node.parent is not None:
            location = node.location
            if location[str]:
                values.append(location[str])
            if location[int]:
                values.append(location[int])
            node = node.parent

        values.reverse()

        return itertools.product(*values)

    def get_data_by_path(self, path):
        selector = self._data
        for _id in path:
            selector = selector[_id]

        return selector

    @staticmethod
    def load_data(_data):
        data_type = type(_data)
        if data_type in (dict, list):
            pass
        elif data_type is str and _data.endswith(".json"):
            _data = json.load(open(_data))
        else:
            raise Exception

        return _data
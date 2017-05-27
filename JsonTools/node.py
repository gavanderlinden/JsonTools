import hashlib


class Node(object):

    def __init__(self, _parent, _type, _master=None):
        self._master = _master
        self._parent = _parent
        self._type = _type
        self._meta = self.node_meta()
        self._children = {}

    @property
    def master(self):
        return self._master

    @property
    def parent(self):
        return self._parent

    @property
    def children(self):
        return self._children

    @property
    def meta(self):
        return self._meta

    @property
    def location(self):
        return self._meta["loc"]

    def add_node(self, data, location):
        node_type = type(data)

        if node_type is list:

            parent = self._add_list_node(data, location)

            for i, value in enumerate(data):
                Node(parent, "list", self._master).add_node(value, i)

        elif node_type is dict:

            parent = self._add_dict_node(data, location)

            for new_key, value in sorted(data.items()):
                Node(parent, "dict", self._master).add_node(value, new_key)

    def _add_list_node(self, node, location):

        _id = self.list_to_schema(node)

        meta = self.parent.add_child(_id, self, location)  # second time this parent is different

        return self.parent.children.get(_id)

    def _add_dict_node(self, node, location):

        _keys = sorted(node)
        _id = self.hash_keys("|".join(_keys))

        meta = self.parent.add_child(_id, self, location, _keys)
        meta["keys"] = _keys

        return self.parent.children.get(_id)

    def add_child(self, _id, child, location, keys=None):
        if _id not in self.children:
            self.children[_id] = child

            if keys:
                self.children[_id].meta["keys"] = keys
                for key in keys:
                    self.master.master_keys[key].append(
                        self.children[_id]
                    )

        meta = self.children[_id].meta
        meta["cnt"] += 1

        if location:

            location_type = type(location)
            location_meta = meta["loc"][location_type]

            if location not in location_meta:
                location_meta.append(location)

        return meta

    @staticmethod
    def list_to_schema(_list, early_stopping=100):
        buffer = []
        cnt = 0
        previous_type = "NOT_SET"
        for i, value in enumerate(_list):
            value_type = type(value)

            if value_type == previous_type:
                cnt += 1
            else:
                if cnt > 0:
                    buffer.append((previous_type, cnt))
                    cnt = 0
                previous_type = value_type
                cnt += 1

            if cnt >= early_stopping:
                cnt += (len(_list) - (i + 1))
                break

        if cnt > 0:
            buffer.append((value_type, cnt))

        return tuple(buffer)

    @staticmethod
    def hash_keys(sorted_keys_string):
        return hashlib.md5(sorted_keys_string.encode()).hexdigest()

    @staticmethod
    def node_meta(keys=None):
        return {
            "keys": keys,
            "loc": {
                int: [],
                str: [],
            },
            "cnt": 0,
        }

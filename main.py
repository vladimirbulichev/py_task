import copy


class Tree:
    """класс дерева, реализванного ввиде словаря"""
    def __init__(self, root=None):
        self.root = root
        self.tree = {self.root: {}}
        self.paths = {self.root: []}

    def add_node(self, parent, child):
        """
        :param parent: код родителя
        :param child: код потомка
        :return: bool признак успеха добавления
        """
        if parent not in self.paths:
            return False
        path = self.paths.get(parent)
        child_path = copy.copy(path)
        child_path.append(parent)
        self.paths[child] = child_path
        a = self.tree
        for key in child_path:
            a = a.get(key)
        a[child] = {}
        return True

    def as_dict(self):
        return self.tree.get(self.root)


def to_tree(arr):
    """
    :param arr: массив кортежей (родитель, потомок)
    :return: словарь дерева
    """
    t = Tree()
    for (parent, child) in arr:
        t.add_node(parent, child)
        if not t.add_node(parent, child):
            raise Exception('node=', parent, 'not found')
    return t.as_dict()


source = [
    (None, 'a'),
    (None, 'b'),
    (None, 'c'),
    ('a', 'a1'),
    ('a', 'a2'),
    ('a2', 'a21'),
    ('a2', 'a22'),
    ('b', 'b1'),
    ('b1', 'b11'),
    ('b11', 'b111'),
    ('b', 'b2'),
    ('c', 'c1'),
]

expected = {
    'a': {'a1': {}, 'a2': {'a21': {}, 'a22': {}}},
    'b': {'b1': {'b11': {'b111': {}}}, 'b2': {}},
    'c': {'c1': {}},
}

assert to_tree(source) == expected

"""Module containing fucntions for manipulating the dask graph
"""
from dask.base import tokenize


def isleaf(obj):

    try:
        len(obj)
    except TypeError:
        return True
    else:
        return False


def filter_tree(dsk, key):
    """Return the subtree consisting only of Target objects
    """
    stack = [(None, dsk[key])]
    graph = {}

    while stack:

        parent, node = stack.pop()

        if node in dsk:
            node = dsk[node]

        if isleaf(node):
            continue
        else:
            head, rest = node[0], node[1:]

        if isinstance(head, Targeted):
            graph[head] = []

            if parent is not None:
                graph[parent].append(head)

            for arg in rest:
                stack.append((head, arg))
        else:
            for arg in node:
                stack.append((parent, arg))

    return graph


def _unfuse_targets_tuple(tup, match):
    top_head = tup[0]
    out = []
    stack = [(tup, out, (None, None, None), True)]
    new_keys = {}

    while stack:
        tup, new_tup, (parent, idx, make_tuple), top = stack.pop()

        if make_tuple:
            parent[idx] = tuple(new_tup)
            continue

        if isinstance(tup, tuple):
            if match(tup[0]) and (not top):
                head = tokenize(*tup)
                new_keys[head] = tup
                new_tup.append(head)
            else:
                new_tup.append([])
                new_tup, parent, idx = new_tup[-1], new_tup, len(new_tup) - 1
                # process rest
                # add instruction to wrap data in tuple
                stack.append((None, new_tup, (parent, idx, True), None))
                for elm in tup[::-1]:
                    stack.append((elm, new_tup, (None, None, None), False))
        else:
            new_tup.append(tup)

    out = tuple(out[0])

    return out, new_keys


def unfuse_match(dsk, match):
    """Move matched objects to be top-level keys, this makes it much easier to process these keys
    """
    dsk2 = {}
    for key in dsk:
        dsk2[key] = dsk[key]

    stack = list(dsk)
    while stack:
        key = stack.pop()
        val = dsk2[key]
        if isinstance(val, tuple):
            new_tup, new_keys = _unfuse_targets_tuple(val, match)

            # update the output dict
            dsk2[key] = new_tup
            dsk2.update(new_keys)

            # need to process the new_keys
            stack.extend(new_keys)

    return dsk2


def flatten(x):
    """Flatten nested list structure with tuples at the bottom"""
    stack = [x]
    flat = []
    while stack:
        node = stack.pop()
        if isinstance(node, tuple):
            flat.append(node)
        else:
            for x in node:
                stack.append(x)
    return flat

"""Leakage-control groups: which images must always land in the same split. Pure Python plus scipy,
tested without Spark (tests/test_leakage.py).
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

# Images sharing a value in any of these columns are linked. Linked images, and images linked to
# those, and so on, end up in one group.
LINK_COLUMNS = {"duplicate": "source_checksum", "lesion": "lesion_id", "patient": "patient_id"}


def assign_groups(images: list[dict]) -> dict[tuple[str, str], tuple[str, str]]:
    """Group images so that any two sharing the same bytes, lesion or patient, directly or through a
    chain of such links, are in the same group. Links never cross datasets, since each dataset
    issues its own lesion and patient IDs (ADR 003).

    `images` are dicts with dataset_key, image_id, source_checksum, lesion_id and patient_id (the
    last two may be None). Returns {(dataset_key, image_id): (group_id, group_type)}. group_id is
    "<dataset_key>:<lowest image_id in the group>"; group_type names the kinds of link that formed
    the group, e.g. "lesion+patient", or "singleton" for an image linked to nothing.
    """
    members_by_value: dict[tuple, list[int]] = defaultdict(list)
    for index, image in enumerate(images):
        for link, column in LINK_COLUMNS.items():
            if image.get(column) is not None:
                members_by_value[(image["dataset_key"], link, image[column])].append(index)

    sources, targets = [], []
    links_by_image: dict[int, set[str]] = defaultdict(set)
    for (_, link, _), members in members_by_value.items():
        if len(members) < 2:
            continue
        sources.extend([members[0]] * (len(members) - 1))
        targets.extend(members[1:])
        for member in members:
            links_by_image[member].add(link)

    image_count = len(images)
    graph = coo_matrix((np.ones(len(sources)), (sources, targets)), shape=(image_count, image_count))
    _, component_by_image = connected_components(graph, directed=False)

    members_by_component: dict[int, list[int]] = defaultdict(list)
    for index, component in enumerate(component_by_image):
        members_by_component[int(component)].append(index)

    groups = {}
    for members in members_by_component.values():
        dataset_key = images[members[0]]["dataset_key"]
        group_id = f"{dataset_key}:{min(images[member]['image_id'] for member in members)}"
        group_type = "+".join(sorted(set().union(*(links_by_image[member] for member in members)))) or "singleton"
        for member in members:
            groups[(dataset_key, images[member]["image_id"])] = (group_id, group_type)
    return groups

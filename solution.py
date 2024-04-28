from list_utils.functional_list import FunctionalList as Fl
from list_utils.unpack import unpack as _

EDGE = ("a", "b")
LABELLED = ("id", "lab", "obs")
LABELLED_PAIR = (LABELLED, ("n_id", "n_lab", "n_obs"))


def solve(edges: Fl):
    points, edges, labelled = initialize(edges)
    any_change = True
    steps = 0
    while any_change:
        steps += 1
        print(f"step: {steps}")
        labelled, change_info = step(labelled, edges)
        any_change = any(change_info)
    return get_metadata(labelled)


def initialize(edges: Fl) -> (Fl, Fl, Fl):
    edges = get_two_way_edges(edges)
    points = get_points(edges)
    labelled = points.map(lambda a: (a, a, a))
    with_n = join_with_neighbours(labelled, edges)
    labelled = with_n.map(
        _(LABELLED_PAIR, lambda id, lab, obs, n_id, n_lab, n_obs: (id, min(lab, n_lab), min(lab, n_lab))))
    labelled = reduce_duplicates(labelled)
    return points, edges, labelled


def step(labelled: Fl, edges: Fl) -> (Fl, Fl):
    with_n = join_with_neighbours(labelled, edges)
    labelled, change_info = update_label_from_neighbours(with_n)
    labelled = get_observed_observed(labelled)
    labelled = update_label_from_observed(labelled)
    return labelled, change_info


def get_two_way_edges(edges: Fl) -> Fl:
    return edges.flat_map(_(EDGE, lambda a, b: ((a, b), (b, a))))


def get_points(two_way_edges: Fl) -> Fl:
    return two_way_edges.map(_(EDGE, lambda a, b: a)).distinct()


def join_with_neighbours(labelled: Fl, edges: Fl) -> Fl:
    l = edges.join_by_custom_key(labelled, _(EDGE, lambda a, b: a), _(LABELLED, lambda id, lab, obs: id))
    r = edges.join_by_custom_key(labelled, _(EDGE, lambda a, b: b), _(LABELLED, lambda id, lab, obs: id))
    joined = l.join_by_key(r).map(lambda x: x[1])
    return joined


def update_label_from_neighbours(labelled_pair: Fl) -> (Fl, Fl):
    updated_with_change_info = labelled_pair.flat_map(
        _(
            LABELLED_PAIR,
            lambda id, lab, obs, n_id, n_lab, n_obs:
            Fl([((id, lab, obs), False)]) if lab <= n_lab else Fl([((id, n_lab, obs), True), ((obs, lab, n_id), True)])
        )
    )
    change_info = updated_with_change_info.map(lambda x: x[1])
    updated = updated_with_change_info.map(lambda x: x[0])
    updated = reduce_duplicates(updated)

    return updated, change_info


def reduce_duplicates(labelled: Fl) -> Fl:
    return labelled \
        .map(_(LABELLED, lambda id, lab, obs: (id, (lab, obs)))) \
        .reduce_by_key(lambda l, r: l if l[0] <= r[0] else r) \
        .map(_(("id", ("lab", "obs")), lambda id, lab, obs: (id, lab, obs)))


def join_with_observed(labelled: Fl) -> Fl:
    return labelled.join_by_custom_key(labelled, _(LABELLED, lambda obs: obs), _(LABELLED, lambda id: id))


def get_observed_observed(labelled: Fl) -> Fl:
    with_o = join_with_observed(labelled)
    return with_o.map(
        _(
            LABELLED_PAIR,
            lambda id, lab, obs, n_id, n_lab, n_obs:
            (id, lab, n_obs)
        )
    )


def update_label_from_observed(labelled: Fl) -> Fl:
    with_o = join_with_observed(labelled)
    return with_o.map(
        _(
            LABELLED_PAIR,
            lambda id, lab, obs, n_id, n_lab, n_obs:
            (id, min(lab, n_lab), obs)
        )
    )


def get_metadata(labelled: Fl) -> Fl:
    return labelled\
        .map(_(LABELLED, lambda lab: (lab, 1)))\
        .reduce_by_key(lambda a, b: a + b)

from pkg_resources import get_distribution


def version():
    """Returns the current Hnswlib version.

    Returns
    -------
    str
        The current Hnswlib version.
    """
    return get_distribution("hnswlib-spark").version

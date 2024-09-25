# Databricks notebook source
def plot_ecdf(x, plot_kwargs=None, show_graph=False):
    """
    from `rosey` package
    Create the plot of the empricial distribution function
    >>> import numpy as np
    >>> import matplotlib.pyplot as graph
    >>> plot_ecdf(np.random.normal(100, 15, size=100), {'label': 'blah'})
    >>> graph.show()
    :param x:
    :param plot_kwargs:
    :param show_graph:
    :return:
    """
    import matplotlib.pyplot as graph
    from utils import ecdf

    cdf, x = ecdf(x)
    plot_kwargs = dict() if plot_kwargs is None else plot_kwargs

    graph.plot(x, cdf, **plot_kwargs)

    if show_graph:
        graph.show()

# COMMAND ----------



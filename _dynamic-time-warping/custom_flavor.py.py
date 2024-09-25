# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### This notebook contains the source code for the `custom_flavor` module which is sourced in the Custom Flavor Definition Notebook 

# COMMAND ----------

import os
import shutil

# DTW library import - necessary for defining this custom flavor
import _ucrdtw

import pandas as pd
import yaml

import mlflow.tracking
from mlflow import pyfunc
from mlflow.exceptions import MlflowException
from mlflow.models import Model
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST
from mlflow.utils import PYTHON_VERSION
from mlflow.utils.environment import _mlflow_conda_env
from mlflow.utils.model_utils import _get_flavor_configuration


DEFAULT_CONDA_ENV = _mlflow_conda_env(
    additional_conda_deps=None,
    additional_pip_deps=None,
    additional_conda_channels=None,
)

# Define flavor name for use in encoding/serializing model in addition to loader_module name
FLAVOR_NAME = "dtw"


# This is the managed way to save a model (in contrast to save_model)
def log_model(dtw_model, artifact_path, conda_env=None, **kwargs):
    """
    Log a DTW model as an MLflow artifact for the current run.

    :param dtw_model: DTW model to be saved.
    :param artifact_path: Run-relative artifact path.
    :param conda_env: Path to a Conda environment file. If provided, this
                      defines the environment for the model. At minimum, it
                      should specify python and mlflow with
                      appropriate versions.
    :param kwargs: kwargs
    """
    current_module = __import__(__name__)
    Model.log(artifact_path=artifact_path, flavor=current_module,
              dtw_model=dtw_model, conda_env=conda_env, **kwargs)


def save_model(dtw_model, path, conda_env=None, mlflow_model=Model(), **kwargs):
    """
    Save a DTW model to a path on the local file system.

    :param dtw_model: DTW model to be saved.
    :param path: Local path where the model is to be saved.
    :param conda_env: Path to a Conda environment file. If provided, this
                      describes the environment this model should be run in.
                      At minimum, it should specify python and mlflow
                      with appropriate versions.
    :param mlflow_model: :py:mod:`mlflow.models.Model` this flavor is being added to.
    :param kwargs: kwargs to pass
    """
    path = os.path.abspath(path)
    if os.path.exists(path):
        raise RuntimeError("Path '{}' already exists".format(path))
    os.makedirs(path)
    model_path = os.path.join(path, "model.pth")

    # Save DTW model using dill to serialize DTW parameters
    import dill
    os.makedirs(model_path)
    model_output_dtw = open(model_path + '/dtw.pkl', 'wb')
    dill.dump(dtw_model, model_output_dtw)

    model_file = os.path.basename(model_path)

    conda_env_subpath = "conda.yaml"
    if conda_env is not None:
        shutil.copyfile(conda_env, os.path.join(path, conda_env_subpath))
    else:
        with open(os.path.join(path, conda_env_subpath), "w") as f:
            yaml.safe_dump(DEFAULT_CONDA_ENV, stream=f, default_flow_style=False)

    mlflow_model.add_flavor(FLAVOR_NAME, model_data=model_file,
                            dtw_version=3)
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.dtw",
                        data=model_file, env=conda_env_subpath)
    mlflow_model.save(os.path.join(path, "MLmodel"))


def _load_model(path, **kwargs):
    import dill
    model_dict = dill.load(open(path + '/dtw.pkl', 'rb'))
    return(model_dict)

def load_model(path, run_id=None, **kwargs):
    """
    Load a SpaCy model from a local file (if ``run_id`` is ``None``) or a run.

    :param path: Local filesystem path or run-relative artifact path to the model saved
                 by :py:func:`dtw_flavor.log_model`.
    :param run_id: Run ID. If provided, combined with ``path`` to identify the model.
    :param kwargs: kwargs to pass to ``dtw_flavor.load`` method.
    """
    if run_id is not None:
        path = mlflow.tracking.utils._get_model_log_dir(model_name=path, run_id=run_id)
    path = os.path.abspath(path)
    flavor_conf = _get_flavor_configuration(model_path=path, flavor_name=FLAVOR_NAME)
    dtw_model_artifacts_path = os.path.join(path, flavor_conf['model_data'])
    return FlavorWrapper(_load_model(path=dtw_model_artifacts_path, **kwargs))


def _load_pyfunc(path, **kwargs):
    """
    Load PyFunc implementation. Called by ``pyfunc.load_pyfunc``.
    """
    return FlavorWrapper(_load_model(os.path.abspath(path), **kwargs))

class FlavorWrapper(object):
    """
    Wrapper class that creates a predict function such that
    predict(data: pd.DataFrame) -> model's output as pd.DataFrame (pandas DataFrame)
    """
    def __init__(self, dtw_model):
        self.dtw_model = dtw_model

    def predict(self, data):
        def get_keyed_values(s):
            return (s[0], s[1:])

        def compute_distance(row):
            pattern = self.dtw_model['pattern']
            stretch_factor = self.dtw_model['stretch_factor']
            return (row[0], _ucrdtw.ucrdtw(list(row[1][0:52]), list(pattern), stretch_factor, True)[1])

        import numpy as np

        ts_values = pd.DataFrame(np.apply_along_axis(get_keyed_values, 1, data.values))
        distances = pd.DataFrame(np.apply_along_axis(compute_distance, 1, ts_values.values))
        distances.columns = ['pcode', 'dtw_dist']
        return (distances)

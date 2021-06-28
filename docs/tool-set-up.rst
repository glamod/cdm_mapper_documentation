===========
Tool set up
===========

The cdm tool is a pure Python package, but it has a few dependencies that rely in a specific python and module version. The tool has been tested with Python version 3.7 on Linux and Mac OS systems.

1. Clone the repository
~~~~~~~~~~~~~~~~~~~~~~~~

Make sure that you have a designated folder or directory to store the module and clone the latest version via::

      $ cd /to_your_designated_folder/
      $ git clone git@git.noc.ac.uk:brecinosrivas/cdm-mapper.git --branch master --single-branch cdm

.. _git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

.. warning:: Don't forget to do it as a ``--single-branch cdm`` otherwise you wont be able to use it as a python module.

2. Install a python environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this you can use and install `pyenv <https://github.com/pyenv/pyenv>`_ and create a new virtual environment
with a the python version needed (**3.7.3**) using `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_.

If you install pyenv and pyenv-virtualenv you can create an environment with a fix python version::

    $ pyenv install 3.7.3
    $ pyenv virtualenv 3.7.3 cdm_mapper_env
    $ pyenv activate cdm_mapper_env

As another option you can use conda. See the `conda docs <https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands>`_
for more information about how to create an environment from the command line.

Or you can do what I usually do (much faster), install `mamba <https://github.com/mamba-org/mamba>`_.

.. warning:: **Make sure you activate your environment before continuing**

3. Install dependencies
~~~~~~~~~~~~~~~~~~~~~~~

If you used **pyenv** for your environment, once activated you can install the dependencies using `pip <https://pip.pypa.io/en/stable/>`_::

    $ pip install dask==1.1.4 datashader==0.7.0 matplotlib==3.0.3 numpy==1.16.5 pandas==0.24.2 requests==2.21.0 xarray==0.12.1 msgpack==0.5.6 scipy==1.6.0
    $ pip install cloudpickle

Check the conda or mamba documentation to install dependencies via those tools.

.. warning:: **The pandas version and the ``cloudpickle`` library are particularly important since these libraries need to be compatible with the way of importing the json module used in the code.**

4. Install ``mdf_reader`` toolbox
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When installing the `mdf_reader <https://mdf-reader.readthedocs.io/en/mdf_reader/tool-set-up.html#>`_ toolbox you don't need to create a new python or conda environment, just install the tool by cloning the repository **in the same directory where you have stored the cdm toolbox via**::

    $ cd /to_your_designated_folder/
    $ git clone git@git.noc.ac.uk:brecinosrivas/mdf_reader.git

.. warning:: **Make sure that both repositories cdm and mdf_reader are stored in the same directory**

5. Optional step: install jupyter notebooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install `jupyter notebook <https://jupyter.org/install>`_ and `IPython <https://jupyter.readthedocs.io/en/latest/install.html>`_ for an easy overview of the tool and to make use of the tutorials under ``~/cdm/docs/notebooks``::

    $ pip install notebook
    $ pip install ipykernel

Check the libraries documentation in the links above to install them via conda or mamba.

Add a new kernel to load your notebooks with the right environment (``cdm_mapper_env``) run::

    $ python -m ipykernel install --user --name=cdm_mapper_env
    $ jupyter notebook

When you open the notebook, make sure you select the kernel or environment with the name ``cdm_mapper_env``. You can also
test the notebook by adding and executing the following code in a jupyter-notebook cell::

    from platform import python_version
    import sys
    print(python_version())
    print(sys.executable)
    print(sys.version)
    print(sys.version_info)

And you should see the following information for your ``cdm_mapper_env``::

    /Users/username/.pyenv/versions/3.7.3/envs/cdm_mapper_env/bin/python
    3.7.3 (default, Feb  4 2021, 14:32:54)
    [Clang 12.0.0 (clang-1200.0.32.28)]
    sys.version_info(major=3, minor=7, micro=3, releaselevel='final', serial=0)

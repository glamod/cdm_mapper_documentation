# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'cdm-mapper'
copyright = '2021, David Berry, Irene Perez Gonzalez and Beatriz Recinos'
author = 'David Berry, Irene Perez Gonzalez and Beatriz Recinos'

# The full version, including alpha/beta/rc tags
release = 'v1.3'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
# extensions = []

# extensions.append('autoapi.extension')
extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.autosummary',
              'sphinx.ext.viewcode',
              'autoapi.extension',
              'sphinx.ext.napoleon',
              'sphinx_autodoc_typehints']


autoapi_type = 'python'
autoapi_dirs = ['../']
add_module_names = False
autoapi_keep_files = False
autodoc_typehints = "description"

#autoapi_options = ['members', 'undoc-members', 'private-members']
autoapi_options = ['members', 'undoc-members', 'private-members', 'show-inheritance',
                    'show-module-summary', 'special-members', 'imported-members']
autoapi_ignore = ['*conf*', '*icoads_*', '*pub47_noc*', '*template*']


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

pygments_style = 'sphinx'
# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

html_theme_options = {
    'logo_only': True,
    'display_version': False,
    "collapse_navigation": False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
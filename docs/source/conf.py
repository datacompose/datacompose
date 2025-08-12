"""Sphinx configuration for Datacompose documentation."""

import sys
from pathlib import Path

# Add project root to Python path for autodoc
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# -- Project information -----------------------------------------------------
project = "Datacompose"
copyright = "2024, Datacompose Team"
author = "Datacompose Team"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_tabs.tabs",
    "sphinx_click",
]

# MyST Parser configuration
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "tasklist",
]

# Add support for both .rst and .md files
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# The master toctree document
master_doc = "index"

# List of patterns to ignore
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#7C4DFF",
        "color-brand-content": "#7C4DFF",
        "color-api-background": "#F5F5F5",
    },
    "dark_css_variables": {
        "color-brand-primary": "#9C88FF",
        "color-brand-content": "#9C88FF",
        "color-api-background": "#1A1A1A",
    },
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

html_title = "Datacompose Documentation"
html_logo = None  # Add path to logo if you have one
html_favicon = None  # Add path to favicon if you have one

# Static files
html_static_path = ["_static"]

# -- Extension configuration -------------------------------------------------

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_type_aliases = None

# Intersphinx mapping to other documentation
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
}

# Copy button configuration
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
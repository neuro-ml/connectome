import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# -- Project information -----------------------------------------------------

project = 'connectome'
copyright = '2021, NeuroML Group'
author = 'NeuroML Group'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']
html_static_path = ['_static']
exclude_patterns = []

html_theme = 'sphinx_rtd_theme'

# autodoc
autodoc_inherit_docstrings = False
autodoc_member_order = 'bysource'
default_role = 'any'

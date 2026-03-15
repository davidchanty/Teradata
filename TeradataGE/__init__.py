"""
TeradataGE Package
~~~~~~~~~~~~~~~~~~

This package is a python wrapper to install and call the in-database graph functions
Basic usage:
  from TeradataGE import td_graph_function, configure

  # Setup Teradata Graph Engine database location
  configure.graph_install_location = "[database name]"

  # Define a new class for Graph objects: including Edge and Node tables
  graph_obj = td_graph_function.td_graph_object()

...
"""
Connectome is a framework for datasets management with strong emphasis on simplicity, composability and reusability.

# Features

* Self-consistency: connectome encourages data transformations that keep entries' fields consistent
* Caching: transformations' caching works out of the box and supports both caching to RAM and to Disk
* Automatic cache invalidation: connectome tracks all the changes made to a dataset and automatically invalidates the
  cache when something changes, making sure that your cache is always consistent with the data
* Invertible transformations: write consistent pre- and post- processing to build production-ready pipelines

# Install

```shell
git clone https://github.com/neuro-ml/connectome.git
cd connectome
pip install -e .
# or simply
pip install git+https://github.com/neuro-ml/connectome.git
```

# Acknowledgements

Some parts of our automatic cache invalidation machinery vere heavily inspired by
the [cloudpickle](https://github.com/cloudpipe/cloudpickle) project.

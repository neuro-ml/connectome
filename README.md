Connectome is a framework for datasets management with strong emphasis on simplicity, composability and reusability.

# Features

* Self-consistency: connectome encourages data transformations that keep entries' fields consistent
* Caching: transformations' caching works out of the box and supports both caching to RAM and to Disk
* Automatic cache invalidation: connectome tracks all the changes made to a dataset and automatically invalidates the
  cache when something changes, making sure that your cache is always consistent with the data
* Invertible transformations: write consistent pre- and post- processing to build production-ready pipelines

# Install

The simplest way is to get it from PyPi:

```shell
pip install connectome
```

Or if you want to try the latest version from GitHub:

```shell
git clone https://github.com/neuro-ml/connectome.git
cd connectome
pip install -e .

# or let pip handle the cloning:
pip install git+https://github.com/neuro-ml/connectome.git
```

# Getting started

You can check out our `Intro to connectome` series of tutorials [here](https://github.com/neuro-ml/connectome-tutorials)

# Acknowledgements

Some parts of our automatic cache invalidation machinery vere heavily inspired by
the [cloudpickle](https://github.com/cloudpipe/cloudpickle) project.

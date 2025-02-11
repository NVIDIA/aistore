#!/bin/bash

read -r -d '' SDK_PREFIX << EOM
---
layout: post
title: PYTHON SDK
permalink: /docs/python-sdk
redirect_from:
 - /python_sdk.md/
 - /docs/python_sdk.md/
---

AIStore Python SDK is a growing set of client-side objects and methods to access and utilize AIS clusters. This document contains API documentation
for the AIStore Python SDK.

> For our PyTorch integration, please refer to the [PyTorch Docs](https://github.com/NVIDIA/aistore/tree/main/docs/pytorch.md).
For more information, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI)
or see [https://github.com/NVIDIA/aistore/tree/main/python/aistore](https://github.com/NVIDIA/aistore/tree/main/python/aistore).

EOM

export SDK_PREFIX

read -r -d '' TORCH_PREFIX << EOM
---
layout: post
title: PYTORCH
permalink: /docs/pytorch
redirect_from:
 - /pytorch.md/
 - /docs/pytorch.md/
---

The AIStore PyTorch integration is a growing set of datasets, samplers, and more that allow you to use easily add AIStore support
to a codebase using PyTorch. This document contains API documentation for the AIStore PyTorch integration.

For usage examples, please see:
* [AIS Plugin for PyTorch](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md)
* [Jupyter Notebook Examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/aisio-pytorch/)

![PyTorch Structure](/docs/images/pytorch_structure.webp)

EOM

export TORCH_PREFIX

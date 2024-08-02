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

> For our PyTorch integration, please refer to the [Pytorch Docs](https://github.com/NVIDIA/aistore/tree/main/docs/pytorch.md).
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

The AIStore Pytorch integration is a growing set of datasets, samplers, and datapipes that allow you to use easily add AIStore support
to a codebase using Pytorch. This document contains API documentation for the AIStore Pytorch integration.

> For usage examples, please refer to the [Pytorch README](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md).
For more in-depth examples, see our [notebook examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/aisio-pytorch/).

![Pytorch Structure](/docs/images/pytorch_structure.webp)

EOM

export TORCH_PREFIX

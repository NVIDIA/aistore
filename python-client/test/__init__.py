OPENAPI_MISSING_ERR = 'openapi_client missing, you probably forgot to generate the package (refer to https://github.com/NVIDIA/aistore/blob/master/openapi/README.md#how-to-generate-package)'

PYTHON3_NOT_SUPPORTED = 'python3 is not fully supported, (refer to https://github.com/NVIDIA/aistore/blob/master/openapi/README.md#future)'

try:
    import openapi_client
except ImportError as err:
    raise Exception(OPENAPI_MISSING_ERR)

import six
import warnings
if (six.PY3):
    warnings.warn(PYTHON3_NOT_SUPPORTED)

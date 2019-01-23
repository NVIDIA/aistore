#This file should remain in the git repository to inform users that the package can't be installed as is
#it is expected to be replaced when package is generated
NOT_BUILT_ERR = 'package not generated (refer to https://github.com/NVIDIA/aistore/blob/master/swagger/README.md#how-to-generate-package)'

raise Exception(NOT_BUILT_ERR)

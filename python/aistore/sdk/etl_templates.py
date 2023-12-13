# Returns the MD5 sum of the original data as the response.
# pylint: disable=unused-variable
MD5 = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-md5
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_md5:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# Returns "Hello World!" on any request.
# pylint: disable=unused-variable
HELLO_WORLD = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-hello-world
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_hello_world:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command: ["gunicorn", "main:app", "--workers", "20", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
      # command: ["uvicorn", "main:app", "--reload"]
      env:
        - name: ARG_TYPE
          value: "{arg_type}"
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns "Hello World!" on any request.
# pylint: disable=unused-variable
GO_HELLO_WORLD = """
apiVersion: v1
kind: Pod
metadata:
  name: hello-world-go-transformer
  annotations:
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_hello_world_go:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      command: ['./echo', '-l', '0.0.0.0', '-p', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns the original data, with an MD5 sum in the response headers.
# pylint: disable=unused-variable
GO_ECHO = """
apiVersion: v1
kind: Pod
metadata:
  name: echo-go
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_echo_go:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      command: ['./echo', '-l', '0.0.0.0', '-p', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns the original data, with an MD5 sum in the response headers.
# pylint: disable=unused-variable
ECHO = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-echo
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_echo:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command: ["gunicorn", "main:app", "--workers", "20", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"] 
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns the transformed TensorFlow compatible data for the input TAR files. For
# more information on command options, visit
# https://github.com/NVIDIA/ais-etl/blob/master/transformers/tar2tf/README.md.
# pylint: disable=unused-variable
TAR2TF = """
apiVersion: v1
kind: Pod
metadata:
  name: tar2tf
  annotations:
    # Values it can take ["hpull://","hrev://","hpush://"]
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_tar2tf:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      # To enable conversion e.g.
      command: ['./tar2tf', '-l', '0.0.0.0', '-p', '80', '{arg}', '{val}']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# Returns the compressed/decompressed file. For more information on command options, visit
# https://github.com/NVIDIA/ais-etl/blob/master/transformers/compress/README.md.
# pylint: disable=unused-variable
COMPRESS = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-compress
  annotations:
    # Values `communication_type` can take are ["hpull://", "hrev://", "hpush://", "io://"].
    # Visit https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#communication-mechanisms
    # for more details.
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_compress:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
      env:
      # COMPRESS_OPTIONS is a dictionary (JSON string) of additional parameters
      # `mode` and `compression`. For more information on additional parameters, refer to
      # https://github.com/NVIDIA/ais-etl/blob/master/transformers/compress/README.md.
      - name: COMPRESS_OPTIONS
        value: '{compress_options}'
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# pylint: disable=unused-variable
KERAS_TRANSFORMER = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-keras
  annotations:
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_keras:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command: ["gunicorn", "main:app", "--workers", "20", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"] 
      env:
        - name: FORMAT
          value: "{format}"
        - name: TRANSFORM
          value: '{transform}'
        - name: ARG_TYPE
          value: "{arg_type}"
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns the FFMPEG decoded content. For more information on command options, visit
# https://github.com/NVIDIA/ais-etl/blob/master/transformers/ffmpeg/README.md.
# pylint: disable=unused-variable
FFMPEG = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-ffmpeg
  annotations:
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
  - name: server
    image: aistorage/transformer_ffmpeg:latest
    imagePullPolicy: Always
    ports:
    - name: default
      containerPort: 80
    command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
    env:
    # FFMPEG_OPTIONS is a dictionary (JSON string) of FFMPEG decode parameters. For more information on
    # FFMPEG decode parameters, refer to https://ffmpeg.org/ffmpeg.html#Synopsis.
    - name: FFMPEG_OPTIONS
      value: '{ffmpeg_options}'
    readinessProbe:
      httpGet:
        path: /health
        port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# Returns the transformed images using `Torchvision` pre-processing. For more
# information on command options, visit
# https://github.com/NVIDIA/ais-etl/blob/master/transformers/torchvision_preprocess/README.md.
# pylint: disable=unused-variable
TORCHVISION_TRANSFORMER = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-torchvision
  annotations:
    # Values `communication_type` can take are ["hpull://", "hrev://", "hpush://", "io://"].
    # Visit https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#communication-mechanisms
    communication_type: "{communication_type}://"
    wait_timeout: 10m
spec:
  containers:
    - name: server
      image: aistorage/transformer_torchvision:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command:  ["gunicorn", "main:app", "--workers", "20", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
      env:
        - name: FORMAT
        # expected values - PNG, JPEG, etc
          value: "{format}"
        - name: TRANSFORM
          value: '{transform}'
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: ais
          mountPath: /tmp/
  volumes:
    - name: ais
      hostPath:
        path: /tmp/
        type: Directory
"""

# pylint: disable=unused-variable
FACE_DETECTION_TRANSFORMER = """
apiVersion: v1
kind: Pod
metadata:
  name: transformer-face-detection
  annotations:
    communication_type: "{communication_type}://"
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistorage/transformer_face_detection:latest
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 8000
      command:  ["gunicorn", "main:app", "--workers", "5", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
      readinessProbe:
        httpGet:
          path: /health
          port: default
      env:
        - name: FORMAT
          value: "{format}"
        - name: ARG_TYPE
          value: "{arg_type}"
      volumeMounts:
        - name: ais
          mountPath: /tmp/ais
  volumes:
    - name: ais
      hostPath:
        path: /tmp/ais
        type: Directory
"""

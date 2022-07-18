# returns the md5 sum of the original data as the response
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
      image: aistore/transformer_md5:latest
      imagePullPolicy: IfNotPresent
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# returns "Hello World!" on any request.
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
      image: aistore/transformer_hello_world:latest
      imagePullPolicy: IfNotPresent
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py']
      # This is a health check endpoint which one should specify
      # for aistore to determine the health of the ETL container.
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# returns the original data, with an md5 sum in the response headers.
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
      image: aistore/transformer_echo_go:latest
      imagePullPolicy: IfNotPresent
      ports:
        - name: default
          containerPort: 80
      command: ['./echo', '-l', '0.0.0.0', '-p', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# returns the original data, with an md5 sum in the response headers
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
      image: aistore/transformer_echo:latest
      imagePullPolicy: IfNotPresent
      ports:
        - name: default
          containerPort: 80
      command: ['/code/server.py', '--listen', '0.0.0.0', '--port', '80']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

# returns the transformed TensorFlow compatible data for the input tar files
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
      image: aistore/transformer_tar2tf:latest
      imagePullPolicy: IfNotPresent
      ports:
        - name: default
          containerPort: 80
      # To enable conversion e.g.
      command: ['./tar2tf', '-l', '0.0.0.0', '-p', '80', '{key}', '{value}']
      readinessProbe:
        httpGet:
          path: /health
          port: default
"""

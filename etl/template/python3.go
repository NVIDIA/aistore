// Package template provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package template

type (
	// RuntimePython3 implements Runtime for "python3".
	RuntimePython3 struct{}
)

func (r RuntimePython3) Type() string         { return RuntimePy3 }
func (r RuntimePython3) CodeFileName() string { return "code.py" }
func (r RuntimePython3) DepsFileName() string { return "requirements.txt" }
func (r RuntimePython3) PodSpec() string {
	return `
apiVersion: v1
kind: Pod
metadata:
  name: <NAME>
  annotations:
    communication_type: <COMMUNICATION_TYPE>
    wait_timeout: 5m
spec:
  containers:
    - name: server
      image: aistore/python:3
      imagePullPolicy: Always
      ports:
        - name: default
          containerPort: 80
      command: ['sh', '-c', 'pip install --user -r /code/requirements.txt; python /server.py']
      env:
        - name: MOD_NAME
          value: code
        - name: FUNC_HANDLER
          value: transform
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: code
          mountPath: "/code"
  initContainers:
    - name: server-deps
      image: aistore/python:3
      imagePullPolicy: Always
      command: ['sh', '-c', 'cp /src/* /dst/']
      volumeMounts:
        - name: config
          mountPath: "/src"
        - name: code
          mountPath: "/dst"
  volumes:
    - name: config
      configMap:
        name: <NAME>-source
    - name: code
      emptyDir: {}
`
}

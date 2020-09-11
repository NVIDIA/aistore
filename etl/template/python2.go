// Package template provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package template

type (
	// RuntimePython2 implements Runtime for "python2".
	RuntimePython2 struct{}
)

func (r RuntimePython2) Type() string         { return RuntimePy2 }
func (r RuntimePython2) CodeFileName() string { return "code.py" }
func (r RuntimePython2) DepsFileName() string { return "requirements.txt" }
func (r RuntimePython2) PodSpec() string {
	return `
apiVersion: v1
kind: Pod
metadata:
  name: <NAME>
spec:
  containers:
    - name: server
      image: aistore/python:2
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
      image: aistore/python:2
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

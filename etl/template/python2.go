// Package template provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package template

type (
	// RuntimePython2 implements Runtime for "python2".
	RuntimePython2 struct{}
)

func (r RuntimePython2) Type() string        { return RuntimePy2 }
func (r RuntimePython2) CodeEnvName() string { return "AISTORE_CODE" }
func (r RuntimePython2) DepsEnvName() string { return "AISTORE_DEPS" }
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
      ports:
        - name: default
          containerPort: 80
      command: ['sh', '-c', 'python /server.py']
      env:
        - name: MOD_NAME
          value: code
        - name: FUNC_HANDLER
          value: transform
        - name: PYTHONPATH
          value: /runtime
      readinessProbe:
        httpGet:
          path: /health
          port: default
      volumeMounts:
        - name: code
          mountPath: "/code"
        - name: runtime
          mountPath: "/runtime"
  initContainers:
    - name: server-deps
      image: aistore/python:2
      command:
        - 'sh'
        - '-c'
        - | 
          echo "${AISTORE_CODE}" > /dst/code.py
          echo "${AISTORE_DEPS}" > /dst/requirements.txt
          pip install --target="/runtime" -r /dst/requirements.txt
      volumeMounts:
        - name: code
          mountPath: "/dst"
        - name: runtime
          mountPath: "/runtime"
  volumes:
    - name: code
      emptyDir: {}
    - name: runtime
      emptyDir: {}
`
}

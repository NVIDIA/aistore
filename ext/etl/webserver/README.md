# Go ETL Server Framework

This package provides a simple framework for building high-performance ETL web servers in Go, compatible with AIStore.

## Quickstart

### 1. Create a Go ETL Webserver

```go
import 	"github.com/NVIDIA/aistore/ext/etl/webserver"

type EchoServer struct {
	webserver.ETLServer
}

func (*EchoServer) Transform(input io.ReadCloser, _, _ string) (io.ReadCloser, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	input.Close()
	return io.NopCloser(bytes.NewReader(data)), nil
}

func main() {
	svr := &EchoServer{}
	if err := webserver.Run(svr, "0.0.0.0", 8080); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
```

### 2. Build & Push Container

```Dockerfile
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/ ./
RUN go build -ldflags="-s -w" -o echo

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/echo .
EXPOSE 80
ENTRYPOINT ["./echo"]
```

```bash
docker build -t <myrepo>/echo-etl:latest .
docker push <myrepo>/echo-etl:latest
```

### 3. Deploy with Kubernetes

```yaml
# init_spec.yaml
apiVersion: v1
kind: Pod
metadata:
  name: etl-echo
  annotations:
    communication_type: "hpush://"
spec:
  containers:
    - name: server
      image: <myrepo>/echo-etl:latest
      ports: [{ name: default, containerPort: 8000 }]
      command: ["./echo", "-l", "0.0.0.0", "-p", "8000"]
      readinessProbe:
        httpGet: { path: /health, port: default }
```

### 4. Initialize and Run

```bash
ais etl init spec --name my-echo --from-file init_spec.yaml
ais etl bucket my-echo ais://<src-bucket> ais://<dst-bucket>
```

## Examples

* **Echo**
  Returns the original input data on every request.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/go_echo)
* **Hello World**
  Returns `b"Hello World!"` on every request.
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/go_hello_world)
* **FFmpeg**
  Transform audio files into WAV format with control over Audio Channels (`AC`) and Audio Rate (`AR`).
  [Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/go_FFmpeg)

## Endpoints

- `GET /<object-path>`: Fetch and transform an object
- `PUT /<object-path>`: Send object content to be transformed
- `GET /health`: Health check
- `/ws`: Establish WebSocket connection

## Notes

- Direct PUT is supported: when AIS sends the `ais-node-url` header, the transformed result will be streamed directly to the destination.

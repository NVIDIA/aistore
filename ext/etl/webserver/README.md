# Go ETL Server Framework

This package provides a simple framework for building high-performance ETL web servers in Go, compatible with AIStore.

## Getting Started

1. **Implement the `ETLServer` interface** by defining your transformation logic in the `Transform` method.
2. **Start the server** using `webserver.Run()`.

## Example

```go
import (
	"bytes"
	"io"
	"log"

	"github.com/NVIDIA/aistore/ext/etl/webserver"
)

type EchoServer struct {
	webserver.ETLServer
}

func (*EchoServer) Transform(input io.ReadCloser, path, args string) (io.ReadCloser, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	input.Close()
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Interface guard to ensure EchoServer satisfies webserver.ETLServer at compile time
var _ webserver.ETLServer = (*EchoServer)(nil)

func main() {
	svr := &EchoServer{}
	if err := webserver.Run(svr, "0.0.0.0", 8080); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
```

## Endpoints

- `GET /<object-path>`: Fetch and transform an object
- `PUT /<object-path>`: Send object content to be transformed
- `GET /health`: Health check

## Notes

- Direct PUT is supported: when AIS sends the `ais-node-url` header, the transformed result will be streamed directly to the destination.
- FQN input is supported when `ARG_TYPE=fqn` is set.

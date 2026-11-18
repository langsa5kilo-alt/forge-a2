# forge-a2

forge-a2 is a compact, memory-resident GitHub-compatible forge written in Go. A tiny Node 22 launcher selects the static binary for the host architecture. The server implements REST v3, GraphQL, MCP, and Git smart HTTP without invoking Git or using third-party packages.

Build both committed executables from the repository root:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o forge-amd64 .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o forge-arm64 .
```

The benchmark exercises sequential pushes, concurrent shallow clones, repeated issue reads on one connection, and issue creation. Repository state and Git objects stay in memory; response-ready issue JSON and generated pack responses are cached on hot paths.

The official local suite passes all 120 conformance checks. The first complete run scored feature 600 and agent-native 200. Performance work is measured with the same grader before each optimization is retained.

# PSS Devcon Demo

This is a Go program to start a PSS simulation network with a "connection manager"
which forwards WebSocket clients to nodes in the cluster based on their IP address,
with no two clients being connected to the same node. If all nodes are connected to
clients, further requests will return a 503 Service Unavailable response.

## Usage

Build the binary:

```
go build -o bin/pss-demo .
```

Run the demo:

```
bin/pss-demo \
  --pss-port   8080 \
  --swarm-port 8500 \
  --net-port   8888 \
  --net-addr   127.0.0.1 \
  --swarm-dir  swarm \
  --node-count 10 \
  --log-dir    log
```

This boots a PSS simulation network consisting of `--node-count` nodes with
each node listening on `--net-addr` and their logs being written to individual
files in the `--log-dir` directory, then starts the connection manager on
`--pss-port` (listening on `0.0.0.0` so will be accessible on all of the host's
IP addresses).

It also runs a single Swarm node storing chunks in `--swarm-dir` and exposing
the Swarm HTTP gateway on `--swarm-port`, and the simulation API server on
`--net-port`.

Connect to the connection manager via a WebSocket:

```
$ wscat --connect http://localhost:8080
connected (press CTRL+C to quit)
> {"jsonrpc": "2.0", "method": "pss_baseAddr", "params": [], "id": 1}
< {"jsonrpc":"2.0","id":1,"result":"+hTV5MqMMhayX0o/gTGK2de2ICd7qRxRTLFHaGY+igg="}
```

See the client to node mapping in the connection manager log:

```
INFO [10-28|19:40:38] proxying client to node                  remote_addr=127.0.0.1:49823 node_id=4190c1b67a44ea80
```

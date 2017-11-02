package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/docopt/docopt-go"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/swarm/api"
	swarmhttp "github.com/ethereum/go-ethereum/swarm/api/http"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/flynn/flynn/pkg/shutdown"
)

var usage = `
usage: pss-demo [options]

options:
  -p, --pss-port=PORT      Conn manager WebSocket port [default: 8080]
  -s, --swarm-port=PORT    Swarm HTTP gateway port [default: 8500]
  -d, --swarm-dir=DIR      Swarm data directory [default: swarm]
  -n, --node-count=COUNT   Initial number of pss nodes to start [default: 10]
  -l, --log-dir=DIR        Directory to store node logs [default: log]
`[1:]

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	if err := run(); err != nil {
		log.Crit("error running pss demo", "err", err)
	}
}

func run() error {
	v, err := docopt.Parse(usage, os.Args[1:], true, "0.0.1", false)
	if err != nil {
		return err
	}
	args := Args(v)

	// start pss network
	tmpDir, err := ioutil.TempDir("", "pss-demo")
	if err != nil {
		return err
	}
	shutdown.BeforeExit(func() { os.RemoveAll(tmpDir) })
	logDir := args.String("--log-dir")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	adapter := adapters.NewExecAdapter(tmpDir)
	net, err := NewPssSimulation(adapter, args.Int("--node-count"), logDir)
	if err != nil {
		return err
	}
	shutdown.BeforeExit(func() { net.Shutdown() })

	// start Swarm HTTP gateway
	swarmDir := args.String("--swarm-dir")
	if err := os.MkdirAll(swarmDir, 0755); err != nil {
		return err
	}
	dpa, api, err := newSwarmAPI(swarmDir)
	if err != nil {
		return err
	}
	shutdown.BeforeExit(func() { dpa.Stop() })

	swarmSrv := http.Server{
		Addr:    "0.0.0.0:" + args.String("--swarm-port"),
		Handler: swarmhttp.NewServer(api),
	}
	log.Info("Starting Swarm HTTP gateway", "addr", swarmSrv.Addr)
	go func() {
		if err := swarmSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			shutdown.Fatalf("Swarm server exited unexpectedly: %s", err)
		}
	}()
	shutdown.BeforeExit(func() { swarmSrv.Close() })

	// start conn manager
	connSrv := http.Server{
		Addr:    "0.0.0.0:" + args.String("--pss-port"),
		Handler: newConnManager(net),
	}
	log.Info("Starting conn manager", "addr", connSrv.Addr)
	go func() {
		if err := connSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			shutdown.Fatalf("Conn manager server exited unexpectedly: %s", err)
		}
	}()
	shutdown.BeforeExit(func() { connSrv.Close() })

	// shutdown on SIGINT or SIGTERM
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("received signal, exiting...")
	shutdown.Exit()
	return nil
}

func newSwarmAPI(dataDir string) (*storage.DPA, *api.Api, error) {
	hashFn := storage.MakeHashFunc("SHA3")
	localStore, err := storage.NewLocalStore(hashFn, &storage.StoreParams{
		DbCapacity:    20000000,
		Radius:        0,
		ChunkDbPath:   dataDir,
		CacheCapacity: 500,
	})
	if err != nil {
		return nil, nil, err
	}
	dpa := storage.NewDPA(localStore, storage.NewChunkerParams())
	dpa.Start()
	return dpa, api.NewApi(dpa, nil), nil
}

type Args map[string]interface{}

func (args Args) String(flag string) string {
	v, ok := args[flag]
	if !ok {
		panic(fmt.Sprintf("missing flag: %s", flag))
	}
	s, ok := v.(string)
	if !ok {
		panic(fmt.Sprintf("invalid flag: %s=%q", flag, v))
	}
	return s
}

func (args Args) Int(flag string) int {
	i, err := strconv.Atoi(args.String(flag))
	if err != nil {
		panic(fmt.Sprintf("invalid int flag %s: %s", flag, err))
	}
	return i
}

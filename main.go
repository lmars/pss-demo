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
)

var usage = `
usage: pss-demo [options]

options:
  -p, --port=PORT          Conn manager TCP port [default: 8080]
  -n, --node-count=COUNT   Initial number of pss nodes to start [default: 10]
  -l, --log-dir=DIR        Directory to store node logs [default: log]
`[1:]

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

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
	defer os.RemoveAll(tmpDir)
	logDir := args.String("--log-dir")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	adapter := adapters.NewExecAdapter(tmpDir)
	net, err := NewPssSimulation(adapter, args.Int("--node-count"), logDir)
	if err != nil {
		return err
	}
	defer net.Shutdown()

	// setup conn manager
	cm := newConnManager(net)
	srv := http.Server{
		Addr:    "0.0.0.0:" + args.String("--port"),
		Handler: cm,
	}
	// shutdown on SIGINT or SIGTERM
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		log.Info("received signal, exiting...")
		srv.Close()
	}()
	log.Info("Starting conn manager", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
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

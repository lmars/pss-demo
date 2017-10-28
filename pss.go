package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/pss"
	"github.com/ethereum/go-ethereum/swarm/storage"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv5"
)

func NewPssSimulation(adapter adapters.NodeAdapter, nodeCount int, logDir string) (net *simulations.Network, err error) {
	if nodeCount < 2 {
		return nil, fmt.Errorf("Minimum two nodes in network")
	}
	nodes := make([]*simulations.Node, nodeCount)
	net = simulations.NewNetwork(adapter, &simulations.NetworkConfig{
		ID: "pss-demo",
	})
	defer func() {
		if err != nil {
			net.Shutdown()
		}
	}()
	for i := 0; i < nodeCount; i++ {
		node, err := net.NewNodeWithConfig(&adapters.NodeConfig{
			Services: []string{"bzz", "pss"},
		})
		if err != nil {
			return nil, err
		}
		node.Config.LogFile = filepath.Join(logDir, fmt.Sprintf("%s.log", node.ID().TerminalString()))
		if err := net.Start(node.ID()); err != nil {
			return nil, err
		}
		if i > 0 {
			if err := net.Connect(node.ID(), nodes[i-1].ID()); err != nil {
				return nil, err
			}
		}
		nodes[i] = node
	}
	if nodeCount > 2 {
		if err := net.Connect(nodes[0].ID(), nodes[len(nodes)-1].ID()); err != nil {
			return nil, fmt.Errorf("error connecting first and last nodes")
		}
	}
	return
}

func init() {
	adapters.RegisterServices(services)
}

var services = func() adapters.Services {
	kademlias := make(map[discover.NodeID]*network.Kademlia)
	kademlia := func(id discover.NodeID) *network.Kademlia {
		if k, ok := kademlias[id]; ok {
			return k
		}
		addr := network.NewAddrFromNodeID(id)
		params := network.NewKadParams()
		params.MinProxBinSize = 2
		params.MaxBinSize = 3
		params.MinBinSize = 1
		params.MaxRetries = 1000
		params.RetryExponent = 2
		params.RetryInterval = 1000000
		kademlias[id] = network.NewKademlia(addr.Over(), params)
		return kademlias[id]
	}
	return adapters.Services{
		"pss": func(ctx *adapters.ServiceContext) (node.Service, error) {
			cachedir, err := ioutil.TempDir("", "pss-cache")
			if err != nil {
				return nil, fmt.Errorf("create pss cache tmpdir failed", "error", err)
			}
			dpa, err := storage.NewLocalDPA(cachedir)
			if err != nil {
				return nil, fmt.Errorf("local dpa creation failed", "error", err)
			}

			ctxlocal, _ := context.WithTimeout(context.Background(), time.Second)
			w := whisper.New(&whisper.DefaultConfig)
			wapi := whisper.NewPublicWhisperAPI(w)
			keys, err := wapi.NewKeyPair(ctxlocal)
			privkey, err := w.GetPrivateKey(keys)
			pssp := pss.NewPssParams(privkey)
			pssp.MsgTTL = time.Second * 30
			pskad := kademlia(ctx.Config.ID)
			ps := pss.NewPss(pskad, dpa, pssp)
			return ps, nil
		},
		"bzz": func(ctx *adapters.ServiceContext) (node.Service, error) {
			addr := network.NewAddrFromNodeID(ctx.Config.ID)
			hp := network.NewHiveParams()
			config := &network.BzzConfig{
				OverlayAddr:  addr.Over(),
				UnderlayAddr: addr.Under(),
				HiveParams:   hp,
			}
			return network.NewBzz(config, kademlia(ctx.Config.ID), nil), nil
		},
	}
}()

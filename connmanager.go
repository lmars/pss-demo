package main

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"golang.org/x/net/websocket"
)

type connList struct {
	Key      string
	Assigned bool
}

type connManager struct {
	net      *simulations.Network
	mtx      sync.Mutex
	clients  map[string]*simulations.Node
	assigned map[discover.NodeID]struct{}
}

func newConnManager(net *simulations.Network) *connManager {
	return &connManager{
		net:      net,
		clients:  make(map[string]*simulations.Node),
		assigned: make(map[discover.NodeID]struct{}),
	}
}

func (c *connManager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/list" {
		list := []connList{}
		for _, n := range c.net.GetNodes() {
			rpcclient, _ := n.Client()
			var pubkey string
			rpcclient.Call(&pubkey, "pss_getPublicKey")
			listitem := connList{
				Key: pubkey,
			}
			if _, ok := c.assigned[n.ID()]; ok {
				listitem.Assigned = true
			}
			list = append(list, listitem)
		}
		jsonlist, err := json.Marshal(list)
		if err != nil {
			log.Warn("json marshal failed", "err", err)
		} else if len(jsonlist) == 0 {
			jsonlist = []byte("[]")
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Write(jsonlist)
		return

	}
	node, ok := c.getNode(req)
	if !ok {
		log.Warn("no available node for request", "remote_addr", req.RemoteAddr)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	log.Info("proxying client to node", "remote_addr", req.RemoteAddr, "node_id", node.ID())
	websocket.Server{
		Handler: func(conn *websocket.Conn) { node.ServeRPC(conn) },
	}.ServeHTTP(w, req)
}

func (c *connManager) getNode(req *http.Request) (*simulations.Node, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	clientIP, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		log.Error("error parsing RemoteAddr", "remote_addr", req.RemoteAddr, "err", err)
		return nil, false
	}
	//	if node, ok := c.clients[clientIP]; ok {
	//		return node, true
	//	}
	nodes := c.net.GetNodes()
	for _, node := range nodes {
		if _, ok := c.assigned[node.ID()]; !ok {
			c.assigned[node.ID()] = struct{}{}
			c.clients[clientIP] = node
			return node, true
		}
	}
	return nil, false
}

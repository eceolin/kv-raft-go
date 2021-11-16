package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	retainSnapshotCount   = 2
	raftTimeout           = 10 * time.Second
	communicationProtocol = "tcp"
)

type KVStore struct {
	m  map[string]string
	mu sync.Mutex

	raftInstance *raft.Raft
	RaftDir      string
	RaftBind     string

	logger *log.Logger
}

type operation struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func createKVStore() *KVStore {
	return &KVStore{
		m:      make(map[string]string),
		logger: log.New(os.Stderr, "Criado ", log.LstdFlags),
	}
}

func (s *KVStore) Open(id string, single bool) error {

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	address, err := net.ResolveTCPAddr(communicationProtocol, s.RaftBind)
	if err != nil {
		return err
	}

	transportNetwork, err := raft.NewTCPTransport(s.RaftBind, address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("snapshot erro: %s", err)
	}

	var log raft.LogStore = raft.NewInmemStore()
	var stable raft.StableStore = raft.NewInmemStore()

	raftInstance, err := raft.NewRaft(config, (*fsm)(s), log, stable, snapshots, transportNetwork)
	if err != nil {
		return fmt.Errorf("erro: %s", err)
	}
	s.raftInstance = raftInstance

	if single {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transportNetwork.LocalAddr(),
				},
			},
		}
		raftInstance.BootstrapCluster(configuration)
	}

	return nil
}

func (s *KVStore) Join(nodeID, addr string) error {
	s.logger.Printf("Recebido request de entrada de nodo %s no %s", nodeID, addr)

	configFuture := s.raftInstance.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("falha ao buscar configuração do raft: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("Nodo %s no %s já é membro do cluster, ignorando request", nodeID, addr)
				return nil
			}

			future := s.raftInstance.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("Erro para remover nodo %s no %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raftInstance.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("Nodo %s no %s com sucesso!", nodeID, addr)
	return nil
}

type fsm KVStore

func (s *KVStore) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

func (s *KVStore) Set(key, value string) error {
	if s.raftInstance.State() != raft.Leader {
		return fmt.Errorf("Não é o líder")
	}

	c := &operation{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raftInstance.Apply(b, raftTimeout)
	return f.Error()
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c operation
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("erro: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	default:
		panic(fmt.Sprintf("comando não reconhecido: %s", c.Op))
	}
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}
	f.m = o
	return nil
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Release() {}

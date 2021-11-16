package main

import (
	"fmt"
	"os"
)

var raftAddr string

func main() {

	raftDir := "/home/eduardo/Documentos/raft/"
	raftAddr := "localhost:12000"

	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "Diretório não especificado.")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	kv := createKVStore()
	kv.RaftDir = raftDir
	kv.RaftBind = raftAddr
	err := kv.Open("1", true)

	if err != nil {
		panic(fmt.Sprintf("error: %s", err))
	}

	startHttpServer(kv)
}

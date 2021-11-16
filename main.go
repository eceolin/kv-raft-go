package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

var httpAddr string
var raftDir string
var raftAddr string
var masterAddr string
var nodeId string

func init() {
	flag.StringVar(&raftDir, "raftDir", "/tmp/", "raft directory")
	flag.StringVar(&raftAddr, "raftAddr", ":12000", "raft address")
	flag.StringVar(&nodeId, "nodeId", "1", "node id")
	flag.StringVar(&httpAddr, "httpAddr", ":8080", "http addr interface")
	flag.StringVar(&masterAddr, "masterAddr", "", "master addr")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {

	flag.Parse()

	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "Diretório não especificado.")
		os.Exit(1)
	}

	os.MkdirAll(raftDir, 0700)

	kv := createKVStore()
	kv.RaftDir = raftDir
	kv.RaftBind = raftAddr
	err := kv.Open(nodeId, masterAddr == "")

	if err != nil {
		panic(fmt.Sprintf("error: %s", err))
	}

	if masterAddr != "" {
		var jsonStr = []byte(fmt.Sprintln(`{"id":"` + nodeId + `", "addr": "` + "127.0.0.1" + raftAddr + `"}`))

		fmt.Println("json:", string(jsonStr))

		req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/join", masterAddr), bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}

	startHttpServer(kv, httpAddr)

}

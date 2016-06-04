package main

import (
	"fmt"

	"github.com/hnakamur/tcp_pubsubreply_experiment/msg"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func main() {
	job := msg.Job{Targets: []string{"target1", "target2"}}

	b, err := msgpack.Marshal(&job)
	if err != nil {
		panic(err)
	}

	var v msg.Job
	err = msgpack.Unmarshal(b, &v)
	if err != nil {
		panic(err)
	}
	fmt.Println(v)
}

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/hnakamur/protobufio"
)

var usage = `Usage tcp_pubsubreply_experiment [Globals] <Command> [Options]
Commands:
  server    start the server
  worker    start the worker
  request   request a work
Globals Options:
`

var subcommandOptionsUsageFormat = "\nOptions for subcommand \"%s\":\n"

func subcommandUsageFunc(subcommand string, fs *flag.FlagSet) func() {
	return func() {
		flag.Usage()
		fmt.Printf(subcommandOptionsUsageFormat, subcommand)
		fs.PrintDefaults()
	}
}

func main() {
	var help bool
	flag.BoolVar(&help, "h", false, "show help")

	flag.Usage = func() {
		fmt.Print(usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	switch args[0] {
	case "server":
		serverCommand(args[1:])
	case "worker":
		workerCommand(args[1:])
	case "request":
		requestCommand(args[1:])
	default:
		flag.Usage()
		os.Exit(1)
	}
}

func serverCommand(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	fs.Usage = subcommandUsageFunc("server", fs)
	var address string
	fs.StringVar(&address, "address", ":5000", "listen address")
	fs.Parse(args)

	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	var err error
	var job Job
	var buf []byte
	r := protobufio.NewMessageReader(conn)
	buf, _, _, err = r.ReadVarintLenAndMessage(&job, buf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("server received message: %v\n", job)

	results := JobResults{
		Results: make([]*JobResult, len(job.Targets)),
	}
	for i, target := range job.Targets {
		results.Results[i] = &JobResult{
			Target: target,
			Result: "success",
		}
	}
	w := protobufio.NewMessageWriter(conn)
	_, _, err = w.WriteVarintLenAndMessage(&results)
	if err != nil {
		log.Fatal(err)
	}
	conn.Close()
}

func workerCommand(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	fs.Usage = subcommandUsageFunc("worker", fs)
	var address string
	fs.StringVar(&address, "address", "127.0.0.1:5000", "server address")
	fs.Parse(args)

}

func requestCommand(args []string) {
	fs := flag.NewFlagSet("request", flag.ExitOnError)
	fs.Usage = subcommandUsageFunc("request", fs)
	var address string
	fs.StringVar(&address, "address", "127.0.0.1:5000", "server address")
	fs.Parse(args)

	var err error
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	job := Job{
		Targets: []string{"target1", "target2"},
	}
	w := protobufio.NewMessageWriter(conn)
	_, _, err = w.WriteVarintLenAndMessage(&job)
	if err != nil {
		log.Fatal(err)
	}

	var results JobResults
	var buf []byte
	r := protobufio.NewMessageReader(conn)
	buf, _, _, err = r.ReadVarintLenAndMessage(&results, buf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client received message: %v\n", results)

	conn.Close()
}

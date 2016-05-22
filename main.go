package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

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

	s := newServer()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go s.handleConnection(conn)
	}
}

type server struct {
	jobC             chan Job
	dispatchChannels map[string]chan Job
	mu               sync.Mutex
}

func newServer() *server {
	s := &server{
		jobC:             make(chan Job),
		dispatchChannels: make(map[string]chan Job),
	}
	go s.dispatchJob()
	return s
}

func (s *server) registerWorker(workerID string) chan Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := make(chan Job, 1)
	s.dispatchChannels[workerID] = c
	fmt.Printf("server added channel for workerID: %s\n", workerID)
	for workerID, _ := range s.dispatchChannels {
		fmt.Printf("workerID in s.dispatchChannels: %s\n", workerID)
	}
	return c
}

func (s *server) dispatchJob() {
	for {
		fmt.Printf("server waiting job in dispatchJob\n")
		job := <-s.jobC
		fmt.Printf("server received job in dispatchJob: %v\n", job)
		s.mu.Lock()
		for workerID, dispatchC := range s.dispatchChannels {
			fmt.Printf("server sending job in dispatchJob %v to worker %s\n", job, workerID)
			dispatchC <- job
			fmt.Printf("server sent job in dispatchJob %v to worker %s\n", job, workerID)
		}
		s.mu.Unlock()
	}
}

func (s *server) handleConnection(conn net.Conn) {
	var v int64
	var err error
	var buf []byte
	r := protobufio.NewMessageReader(conn)
	w := protobufio.NewMessageWriter(conn)
	defer conn.Close()
	for {
		v, _, err = r.ReadVarint()
		if err != nil {
			log.Fatal(err)
		}
		msgType := MessageType(v)
		fmt.Printf("server received message type: %s\n", msgType)
		switch msgType {
		case MessageType_JobMsg:
			var job Job
			buf, _, _, err = r.ReadVarintLenAndMessage(&job, buf)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("server received Job message: %v\n", job)
			s.jobC <- job
			fmt.Printf("server sent Job to jobC: %v\n", job)
			return
		case MessageType_RegisterWorkerMsg:
			var msg RegisterWorker
			buf, _, _, err = r.ReadVarintLenAndMessage(&msg, buf)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("server received RegisterWorker message: %v\n", msg)
			dispatchC := s.registerWorker(msg.WorkerID)
			fmt.Printf("server registered worker: %v\n", msg.WorkerID)
			for {
				fmt.Printf("server waiting job from dispatchC for worker %s\n", msg.WorkerID)
				job := <-dispatchC
				fmt.Printf("server received job from dispatchC %v. sending it to worker %s\n", job, msg.WorkerID)
				_, err = w.WriteVarint(int64(MessageType_JobMsg))
				if err != nil {
					log.Fatal(err)
				}
				_, _, err = w.WriteVarintLenAndMessage(&job)
				if err != nil {
					log.Fatal(err)
				}

				v, _, err = r.ReadVarint()
				if err != nil {
					log.Fatal(err)
				}
				msgType := MessageType(v)
				fmt.Printf("server received message type %s from worker %s\n", msgType, msg.WorkerID)
				switch msgType {
				case MessageType_JobResultMsg:
					var result JobResult
					buf, _, _, err = r.ReadVarintLenAndMessage(&result, buf)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("server received JobResult message %v from worker %s\n", result, msg.WorkerID)
				}
			}

		}
	}
}

func workerCommand(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	fs.Usage = subcommandUsageFunc("worker", fs)
	var address string
	fs.StringVar(&address, "address", "127.0.0.1:5000", "server address")
	var workerID string
	fs.StringVar(&workerID, "id", "worker1", "worker ID")
	fs.Parse(args)

	var err error
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	r := protobufio.NewMessageReader(conn)
	w := protobufio.NewMessageWriter(conn)

	_, err = w.WriteVarint(int64(MessageType_RegisterWorkerMsg))
	if err != nil {
		log.Fatal(err)
	}
	_, _, err = w.WriteVarintLenAndMessage(&RegisterWorker{WorkerID: workerID})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("worker registered myself: %s\n", workerID)

	var v int64
	var buf []byte
	for {
		fmt.Printf("worker %s waiting message from server.\n", workerID)
		v, _, err = r.ReadVarint()
		if err != nil {
			log.Fatal(err)
		}
		msgType := MessageType(v)
		fmt.Printf("worker %s received message type from server. type: %s\n", workerID, msgType)
		var job Job
		buf, _, _, err = r.ReadVarintLenAndMessage(&job, buf)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("worker %s received Job message: %v\n", workerID, job)

		_, err = w.WriteVarint(int64(MessageType_JobResultMsg))
		if err != nil {
			log.Fatal(err)
		}
		result := JobResult{
			WorkerID: workerID,
			Results:  make([]*TargetResult, len(job.Targets)),
		}
		for i, target := range job.Targets {
			result.Results[i] = &TargetResult{
				Target: target,
				Result: "success",
			}
		}
		_, _, err = w.WriteVarintLenAndMessage(&result)
		if err != nil {
			log.Fatal(err)
		}
	}
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
	defer conn.Close()

	w := protobufio.NewMessageWriter(conn)
	_, err = w.WriteVarint(int64(MessageType_JobMsg))
	if err != nil {
		log.Fatal(err)
	}
	job := Job{
		Targets: []string{"target1", "target2"},
	}
	_, _, err = w.WriteVarintLenAndMessage(&job)
	if err != nil {
		log.Fatal(err)
	}
}

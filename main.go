package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
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
	jobC           chan *Job
	jobResultsC    chan *JobResults
	workerChannels map[string]workerChannel
	mu             sync.Mutex
}

func newServer() *server {
	s := &server{
		jobC:           make(chan *Job),
		jobResultsC:    make(chan *JobResults),
		workerChannels: make(map[string]workerChannel),
	}
	go s.dispatchJob()
	return s
}

type workerChannel struct {
	jobC       chan *Job
	jobSentC   chan bool
	jobResultC chan *JobResult
}

func (s *server) registerWorker(workerID string) workerChannel {
	c := workerChannel{
		jobC:       make(chan *Job, 1),
		jobSentC:   make(chan bool, 1),
		jobResultC: make(chan *JobResult, 1),
	}
	s.mu.Lock()
	s.workerChannels[workerID] = c
	s.mu.Unlock()
	return c
}

func (s *server) dispatchJob() {
	for {
		job := <-s.jobC
		s.mu.Lock()
		for _, workerChannel := range s.workerChannels {
			workerChannel.jobC <- job
		}

		for workerID, workerChannel := range s.workerChannels {
			jobSent := <-workerChannel.jobSentC
			if !jobSent {
				delete(s.workerChannels, workerID)
				fmt.Printf("server unregister worker %s\n", workerID)
			}
		}

		jobResults := &JobResults{
			Results: make([]*JobResult, 0, len(s.workerChannels)),
		}
		for _, workerChannel := range s.workerChannels {
			jobResult := <-workerChannel.jobResultC
			jobResults.Results = append(jobResults.Results, jobResult)
		}
		s.mu.Unlock()
		s.jobResultsC <- jobResults
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()

	rw := newMessageReadWriter(conn)
	for {
		msgType, err := rw.ReadMessageType()
		if err != nil {
			log.Fatal(err)
		}
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("server received Job %v\n", job)
			s.jobC <- job

			jobResults := <-s.jobResultsC
			err = rw.WriteTypeAndJobResults(jobResults)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("server sent JobResults %v to client\n", jobResults)
			return
		case MessageType_RegisterWorkerMsg:
			registerWorker, err := rw.ReadRegisterWorker()
			if err != nil {
				log.Fatal(err)
			}
			workerID := registerWorker.WorkerID
			workerChannel := s.registerWorker(workerID)
			fmt.Printf("server registered worker: %v\n", workerID)
			for {
				job := <-workerChannel.jobC
				err = rw.WriteTypeAndJob(job)
				if err != nil {
					log.Printf("failed to sent Job %v to worker %s, err=%v", job, workerID, err)
					workerChannel.jobSentC <- false
					return
				}
				fmt.Printf("server sent Job %v to worker %v\n", job, workerID)
				workerChannel.jobSentC <- true

				msgType, err := rw.ReadMessageType()
				if err != nil {
					log.Fatal(err)
				}
				switch msgType {
				case MessageType_JobResultMsg:
					jobResult, err := rw.ReadJobResult()
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("server received JobResult %v from worker %s\n", jobResult, workerID)
					workerChannel.jobResultC <- jobResult
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

	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rw := newMessageReadWriter(conn)

	err = rw.WriteTypeAndRegsiterWorker(&RegisterWorker{WorkerID: workerID})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("worker registered myself: %s\n", workerID)

	for {
		msgType, err := rw.ReadMessageType()
		if err != nil {
			log.Fatal(err)
		}
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("worker %s received Job %v\n", workerID, job)

			jobResult := &JobResult{
				WorkerID: workerID,
				Results:  make([]*TargetResult, len(job.Targets)),
			}
			for i, target := range job.Targets {
				jobResult.Results[i] = &TargetResult{
					Target: target,
					Result: "success",
				}
			}
			err = rw.WriteTypeAndJobResult(jobResult)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("worker %s sent JobResult %v\n", workerID, jobResult)
		}
	}
}

func requestCommand(args []string) {
	fs := flag.NewFlagSet("request", flag.ExitOnError)
	fs.Usage = subcommandUsageFunc("request", fs)
	var address string
	fs.StringVar(&address, "address", "127.0.0.1:5000", "server address")
	fs.Parse(args)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	rw := newMessageReadWriter(conn)

	job := &Job{
		Targets: []string{"target1", "target2"},
	}
	err = rw.WriteTypeAndJob(job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client sent Job %v to server.\n", job)

	msgType, err := rw.ReadMessageType()
	if err != nil {
		log.Fatal(err)
	}
	switch msgType {
	case MessageType_JobResultsMsg:
		jobResults, err := rw.ReadJobResults()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("client received JobResults: %v\n", *jobResults)
	}
}

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
	jobResultC chan *JobResult
}

func (s *server) registerWorker(workerID string) workerChannel {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := workerChannel{
		jobC:       make(chan *Job, 1),
		jobResultC: make(chan *JobResult),
	}
	s.workerChannels[workerID] = c
	fmt.Printf("server added channel for workerID: %s\n", workerID)
	for workerID, _ := range s.workerChannels {
		fmt.Printf("workerID in s.workerChannels: %s\n", workerID)
	}
	return c
}

func (s *server) dispatchJob() {
	for {
		fmt.Printf("server waiting job in dispatchJob\n")
		job := <-s.jobC
		fmt.Printf("server received job in dispatchJob: %v\n", job)
		s.mu.Lock()
		for workerID, workerChannel := range s.workerChannels {
			fmt.Printf("server sending Job %v to worker %s channel\n", job, workerID)
			workerChannel.jobC <- job
			fmt.Printf("server sent Job %v to worker %s channel\n", job, workerID)
		}
		jobResults := &JobResults{
			Results: make([]*JobResult, 0, len(s.workerChannels)),
		}
		for workerID, workerChannel := range s.workerChannels {
			fmt.Printf("server waiting JobResult from worker %s channel\n", workerID)
			jobResult := <-workerChannel.jobResultC
			fmt.Printf("server received JobResult %v from worker %s channel\n", jobResult, workerID)
			jobResults.Results = append(jobResults.Results, jobResult)
		}
		s.mu.Unlock()
		fmt.Printf("server sending jobResults to jobResultsC\n")
		s.jobResultsC <- jobResults
		fmt.Printf("server sent jobResults to jobResultsC\n")
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
		fmt.Printf("server received message type: %s\n", msgType)
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("server received Job %v\n", job)
			s.jobC <- job
			fmt.Printf("server sent Job %v to jobC\n", job)

			jobResults := <-s.jobResultsC
			fmt.Printf("server received JobResults %v from jobResultsC\n", jobResults)
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
			fmt.Printf("server received RegisterWorker message: %v\n", registerWorker)
			workerChannel := s.registerWorker(registerWorker.WorkerID)
			fmt.Printf("server registered worker: %v\n", registerWorker.WorkerID)
			for {
				fmt.Printf("server waiting job from dispatchC for worker %s\n", registerWorker.WorkerID)
				job := <-workerChannel.jobC
				fmt.Printf("server received job from dispatchC %v. sending it to worker %s\n", job, registerWorker.WorkerID)
				err = rw.WriteTypeAndJob(job)
				if err != nil {
					log.Fatal(err)
				}

				msgType, err := rw.ReadMessageType()
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("server received message type %s from worker %s\n", msgType, registerWorker.WorkerID)
				switch msgType {
				case MessageType_JobResultMsg:
					jobResult, err := rw.ReadJobResult()
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("server received JobResult %v from worker %s\n", jobResult, registerWorker.WorkerID)
					workerChannel.jobResultC <- jobResult
					fmt.Printf("server sent JobResult %v from worker %s to jobResultC\n", jobResult, registerWorker.WorkerID)
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
		fmt.Printf("worker %s waiting message from server.\n", workerID)
		msgType, err := rw.ReadMessageType()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("worker %s received message type from server. type: %s\n", workerID, msgType)
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("worker %s received Job message: %v\n", workerID, job)

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

	fmt.Printf("client waiting jobResults from server.\n")
	msgType, err := rw.ReadMessageType()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client received message type from server. type: %s\n", msgType)
	switch msgType {
	case MessageType_JobResultsMsg:
		jobResults, err := rw.ReadJobResults()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("client received JobResults: %v\n", *jobResults)
	}
}

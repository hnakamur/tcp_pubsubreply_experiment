package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
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

type jobResultOrErr struct {
	jobResult *JobResult
	err       error
}

type workerChannel struct {
	jobC            chan *Job
	jobResultOrErrC chan *jobResultOrErr
}

func (s *server) registerWorker(workerID string) workerChannel {
	c := workerChannel{
		jobC:            make(chan *Job, 1),
		jobResultOrErrC: make(chan *jobResultOrErr, 1),
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

		jobResults := &JobResults{
			Results: make([]*JobResult, 0, len(s.workerChannels)),
		}
		for workerID, workerChannel := range s.workerChannels {
			jobResultOrErr := <-workerChannel.jobResultOrErrC
			if jobResultOrErr.err != nil {
				delete(s.workerChannels, workerID)
			} else {
				jobResults.Results = append(jobResults.Results, jobResultOrErr.jobResult)
			}
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
			log.Printf("failed to read MessageType, err=%v", err)
			return
		}
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				log.Printf("failed to read Job from client, err=%v", err)
				return
			}
			log.Printf("server received Job %v\n", job)
			s.jobC <- job

			jobResults := <-s.jobResultsC
			err = rw.WriteTypeAndJobResults(jobResults)
			if err != nil {
				log.Printf("failed to write JobResults to client, err=%v", err)
				return
			}
			log.Printf("server sent JobResults %v to client\n", jobResults)
			return
		case MessageType_RegisterWorkerMsg:
			registerWorker, err := rw.ReadRegisterWorker()
			if err != nil {
				log.Printf("failed to read RegisterWorker from worker, err=%v", err)
				return
			}
			workerID := registerWorker.WorkerID
			workerChannel := s.registerWorker(workerID)
			log.Printf("server registered worker: %v\n", workerID)
			for {
				job := <-workerChannel.jobC
				err = rw.WriteTypeAndJob(job)
				if err != nil {
					log.Printf("failed to sent Job %v to worker %s, err=%v", job, workerID, err)
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				log.Printf("server sent Job %v to worker %v\n", job, workerID)

				msgType, err := rw.ReadMessageType()
				if err != nil {
					log.Printf("failed to read MessageType from worker %s, err=%v", workerID, err)
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				switch msgType {
				case MessageType_JobResultMsg:
					jobResult, err := rw.ReadJobResult()
					if err != nil {
						log.Printf("failed to read JobResult from worker %s, err=%v", workerID, err)
						workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
						return
					}
					log.Printf("server received JobResult %v from worker %s\n", jobResult, workerID)
					workerChannel.jobResultOrErrC <- &jobResultOrErr{jobResult: jobResult}
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
	var connectRetryInterval time.Duration
	fs.DurationVar(&connectRetryInterval, "connect-retry-interval", time.Second, "connect retry interval")
	fs.Parse(args)

	for {
		var rw *messageReadWriter
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("worker %s failed to connect server. err=%v", workerID, err)
			goto retryConnect
		}

		rw = newMessageReadWriter(conn)

		err = rw.WriteTypeAndRegsiterWorker(&RegisterWorker{WorkerID: workerID})
		if err != nil {
			log.Printf("worker %s failed to register myself. err=%v", workerID, err)
			goto retryConnect
		}
		log.Printf("worker registered myself: %s\n", workerID)

		for {
			msgType, err := rw.ReadMessageType()
			if err != nil {
				log.Printf("worker %s failed to read MessageType %v. err=%v", workerID, msgType, err)
				goto retryConnect
			}
			switch msgType {
			case MessageType_JobMsg:
				job, err := rw.ReadJob()
				if err != nil {
					log.Printf("worker %s failed to read Job %v. err=%v", workerID, job, err)
					goto retryConnect
				}
				log.Printf("worker %s received Job %v\n", workerID, job)

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
					log.Printf("worker %s failed to sent JobResult %v. err=%v", workerID, jobResult, err)
					goto retryConnect
				}
				log.Printf("worker %s sent JobResult %v\n", workerID, jobResult)
			}
		}
	retryConnect:
		time.Sleep(connectRetryInterval)
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
	log.Printf("client sent Job %v to server.\n", job)

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
		log.Printf("client received JobResults: %v\n", *jobResults)
	}
}

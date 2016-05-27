package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hnakamur/ltsvlog"
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
	var logFilename string
	fs.StringVar(&logFilename, "log", "-", "log filename. default value \"-\" is stdout")
	var debug bool
	fs.BoolVar(&debug, "debug", false, "enable debug log")
	fs.Parse(args)

	logger, err := newLogger(logFilename, debug)
	if err != nil {
		log.Fatal(err)
	}

	s := newServer(address, logger)
	s.Run()
}

func newLogger(filename string, debug bool) (*ltsvlog.LTSVLogger, error) {
	var w io.Writer
	if filename == "-" {
		w = os.Stdout
	} else {
		var err error
		w, err = os.Open(filename)
		if err != nil {
			return nil, err
		}
	}
	return ltsvlog.NewLTSVLogger(w, debug), nil
}

type server struct {
	address        string
	jobC           chan *Job
	jobResultsC    chan *JobResults
	workerChannels map[string]workerChannel
	mu             sync.Mutex
	logger         *ltsvlog.LTSVLogger
}

func newServer(address string, logger *ltsvlog.LTSVLogger) *server {
	s := &server{
		address:        address,
		jobC:           make(chan *Job),
		jobResultsC:    make(chan *JobResults),
		workerChannels: make(map[string]workerChannel),
		logger:         logger,
	}
	go s.dispatchJob()
	return s
}

func (s *server) Run() {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to listen"},
			ltsvlog.LV{"address", s.address},
			ltsvlog.LV{"err", err})
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.logger.ErrorWithStack(ltsvlog.LV{"msg", "error in accept"},
				ltsvlog.LV{"address", s.address},
				ltsvlog.LV{"err", err})
		}
		go s.handleConnection(conn)
	}
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
			s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
				ltsvlog.LV{"err", err})
			return
		}
		switch msgType {
		case MessageType_JobMsg:
			job, err := rw.ReadJob()
			if err != nil {
				s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read Job from client"},
					ltsvlog.LV{"err", err})
				return
			}
			if s.logger.DebugEnabled() {
				s.logger.Debug(ltsvlog.LV{"msg", "server received Job"},
					ltsvlog.LV{"job", job})
			}
			s.jobC <- job

			jobResults := <-s.jobResultsC
			err = rw.WriteTypeAndJobResults(jobResults)
			if err != nil {
				s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to write JobResults to client"},
					ltsvlog.LV{"err", err})
				return
			}
			if s.logger.DebugEnabled() {
				s.logger.Debug(ltsvlog.LV{"msg", "server sent JobResults to client"},
					ltsvlog.LV{"job_results", jobResults})
			}
			return
		case MessageType_RegisterWorkerMsg:
			registerWorker, err := rw.ReadRegisterWorker()
			if err != nil {
				s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read RegisterWorker from worker"},
					ltsvlog.LV{"err", err})
				return
			}
			workerID := registerWorker.WorkerID
			workerChannel := s.registerWorker(workerID)
			if s.logger.DebugEnabled() {
				s.logger.Debug(ltsvlog.LV{"msg", "server registered worker"}, ltsvlog.LV{"worker_id", workerID})
			}
			for {
				job := <-workerChannel.jobC
				err = rw.WriteTypeAndJob(job)
				if err != nil {
					s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to sent Job to worker"},
						ltsvlog.LV{"err", err}, ltsvlog.LV{"job", job},
						ltsvlog.LV{"worker_id", workerID})
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				if s.logger.DebugEnabled() {
					s.logger.Debug(ltsvlog.LV{"msg", "server sent Job to worker"},
						ltsvlog.LV{"job", job}, ltsvlog.LV{"worker_id", workerID})
				}

				msgType, err := rw.ReadMessageType()
				if err != nil {
					s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType from worker"},
						ltsvlog.LV{"err", err},
						ltsvlog.LV{"worker_id", workerID})
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				switch msgType {
				case MessageType_JobResultMsg:
					jobResult, err := rw.ReadJobResult()
					if err != nil {
						s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read JobResult from worker"},
							ltsvlog.LV{"err", err},
							ltsvlog.LV{"worker_id", workerID})
						workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
						return
					}
					if s.logger.DebugEnabled() {
						s.logger.Debug(ltsvlog.LV{"msg", "server received JobResult from worker"},
							ltsvlog.LV{"job_result", jobResult}, ltsvlog.LV{"worker_id", workerID})
					}
					workerChannel.jobResultOrErrC <- &jobResultOrErr{jobResult: jobResult}
				}
			}
		default:
			s.logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
				ltsvlog.LV{"message_type", msgType})
			return
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
	var logFilename string
	fs.StringVar(&logFilename, "log", "-", "log filename. default value \"-\" is stdout")
	var debug bool
	fs.BoolVar(&debug, "debug", false, "enable debug log")
	fs.Parse(args)

	logger, err := newLogger(logFilename, debug)
	if err != nil {
		log.Fatal(err)
	}
	for {
		var rw *messageReadWriter
		conn, err := net.Dial("tcp", address)
		if err != nil {
			logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to connect server"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"address", address},
				ltsvlog.LV{"err", err})
			goto retryConnect
		}

		rw = newMessageReadWriter(conn)

		err = rw.WriteTypeAndRegsiterWorker(&RegisterWorker{WorkerID: workerID})
		if err != nil {
			logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to register myself"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"err", err})
			goto retryConnect
		}
		if logger.DebugEnabled() {
			logger.Debug(ltsvlog.LV{"msg", "worker registered myself"}, ltsvlog.LV{"worker_id", workerID})
		}

		for {
			msgType, err := rw.ReadMessageType()
			if err != nil {
				logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
					ltsvlog.LV{"worker_id", workerID},
					ltsvlog.LV{"err", err})
				goto retryConnect
			}
			switch msgType {
			case MessageType_JobMsg:
				job, err := rw.ReadJob()
				if err != nil {
					logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read Job"},
						ltsvlog.LV{"worker_id", workerID},
						ltsvlog.LV{"err", err})
					goto retryConnect
				}
				if logger.DebugEnabled() {
					logger.Debug(ltsvlog.LV{"msg", "worker received Job"},
						ltsvlog.LV{"worker_id", workerID},
						ltsvlog.LV{"job", job})
				}

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
					logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to send JobResult"},
						ltsvlog.LV{"worker_id", workerID},
						ltsvlog.LV{"job_result", jobResult},
						ltsvlog.LV{"err", err})
					goto retryConnect
				}
				if logger.DebugEnabled() {
					logger.Debug(ltsvlog.LV{"msg", "worker sent JobResult"},
						ltsvlog.LV{"worker_id", workerID},
						ltsvlog.LV{"job_result", jobResult})
				}
			default:
				logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
					ltsvlog.LV{"message_type", msgType})
				return
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
	var logFilename string
	fs.StringVar(&logFilename, "log", "-", "log filename. default value \"-\" is stdout")
	var debug bool
	fs.BoolVar(&debug, "debug", false, "enable debug log")
	fs.Parse(args)

	logger, err := newLogger(logFilename, debug)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to connect server"},
			ltsvlog.LV{"address", address},
			ltsvlog.LV{"err", err})
	}
	defer conn.Close()

	rw := newMessageReadWriter(conn)

	job := &Job{
		Targets: []string{"target1", "target2"},
	}
	err = rw.WriteTypeAndJob(job)
	if err != nil {
		logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to write Job"},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
	logger.Info(ltsvlog.LV{"msg", "client sent Job"},
		ltsvlog.LV{"job", job})

	msgType, err := rw.ReadMessageType()
	if err != nil {
		logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
	switch msgType {
	case MessageType_JobResultsMsg:
		jobResults, err := rw.ReadJobResults()
		if err != nil {
			logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read JobResults"},
				ltsvlog.LV{"err", err})
			os.Exit(1)
		}
		logger.Info(ltsvlog.LV{"msg", "client received JobResults"},
			ltsvlog.LV{"job_results", jobResults})
	default:
		logger.ErrorWithStack(ltsvlog.LV{"msg", "unexpected MessageType"},
			ltsvlog.LV{"message_type", msgType})
		os.Exit(1)
	}
}

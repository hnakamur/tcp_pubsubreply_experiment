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

	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/hnakamur/ltsvlog"
	"github.com/hnakamur/tcp_pubsubreply_experiment/msg"
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
	jobC           chan *msg.Job
	jobResultsC    chan *msg.JobResults
	workerChannels map[string]workerChannel
	mu             sync.Mutex
	logger         *ltsvlog.LTSVLogger
}

func newServer(address string, logger *ltsvlog.LTSVLogger) *server {
	s := &server{
		address:        address,
		jobC:           make(chan *msg.Job),
		jobResultsC:    make(chan *msg.JobResults),
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
	if s.logger.DebugEnabled() {
		s.logger.Debug(ltsvlog.LV{"msg", "server started listening"},
			ltsvlog.LV{"address", s.address})
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
	jobResult *msg.JobResult
	err       error
}

type workerChannel struct {
	jobC            chan *msg.Job
	jobResultOrErrC chan *jobResultOrErr
}

func (s *server) registerWorker(workerID string) workerChannel {
	c := workerChannel{
		jobC:            make(chan *msg.Job, 1),
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

		jobResults := &msg.JobResults{
			Results: make([]msg.JobResult, 0, len(s.workerChannels)),
		}
		for workerID, workerChannel := range s.workerChannels {
			jobResultOrErr := <-workerChannel.jobResultOrErrC
			if jobResultOrErr.err != nil {
				delete(s.workerChannels, workerID)
			} else {
				jobResults.Results = append(jobResults.Results, *jobResultOrErr.jobResult)
			}
		}
		s.mu.Unlock()
		s.jobResultsC <- jobResults
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()

	dec := msgpack.NewDecoder(conn)
	enc := msgpack.NewEncoder(conn)
	for {
		msgType, err := msg.DecodeMessageType(dec)
		if err != nil {
			s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
				ltsvlog.LV{"err", err})
			return
		}
		switch msgType {
		case msg.JobMsg:
			var job msg.Job
			err := msg.DecodeJob(dec, &job)
			if err != nil {
				s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read Job from client"},
					ltsvlog.LV{"err", err})
				return
			}
			if s.logger.DebugEnabled() {
				s.logger.Debug(ltsvlog.LV{"msg", "server received Job"},
					ltsvlog.LV{"job", job})
			}
			s.jobC <- &job

			jobResults := <-s.jobResultsC
			err = msg.EncodeTypeAndJobResults(enc, jobResults)
			if err != nil {
				s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to write JobResults to client"},
					ltsvlog.LV{"err", err})
				return
			}
			if s.logger.DebugEnabled() {
				s.logger.Debug(ltsvlog.LV{"msg", "server sent JobResults to client"},
					ltsvlog.LV{"job_results", *jobResults})
			}
			return
		case msg.RegisterWorkerMsg:
			registerWorker := new(msg.RegisterWorker)
			err := msg.DecodeRegisterWorker(dec, registerWorker)
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
				err = msg.EncodeTypeAndJob(enc, job)
				if err != nil {
					s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to sent Job to worker"},
						ltsvlog.LV{"err", err}, ltsvlog.LV{"job", job},
						ltsvlog.LV{"worker_id", workerID})
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				if s.logger.DebugEnabled() {
					s.logger.Debug(ltsvlog.LV{"msg", "server sent Job to worker"},
						ltsvlog.LV{"job", *job}, ltsvlog.LV{"worker_id", workerID})
				}

				msgType, err := msg.DecodeMessageType(dec)
				if err != nil {
					s.logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType from worker"},
						ltsvlog.LV{"err", err},
						ltsvlog.LV{"worker_id", workerID})
					workerChannel.jobResultOrErrC <- &jobResultOrErr{err: err}
					return
				}
				switch msgType {
				case msg.JobResultMsg:
					var jobResult msg.JobResult
					err := msg.DecodeJobResult(dec, &jobResult)
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
					workerChannel.jobResultOrErrC <- &jobResultOrErr{jobResult: &jobResult}
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
		var dec *msgpack.Decoder
		var enc *msgpack.Encoder
		conn, err := net.Dial("tcp", address)
		if err != nil {
			logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to connect server"},
				ltsvlog.LV{"worker_id", workerID},
				ltsvlog.LV{"address", address},
				ltsvlog.LV{"err", err})
			goto retryConnect
		}
		if logger.DebugEnabled() {
			logger.Debug(ltsvlog.LV{"msg", "worker connected to the server"},
				ltsvlog.LV{"address", address})
		}

		dec = msgpack.NewDecoder(conn)
		enc = msgpack.NewEncoder(conn)

		err = msg.EncodeTypeAndRegisterWorker(enc, &msg.RegisterWorker{WorkerID: workerID})
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
			msgType, err := msg.DecodeMessageType(dec)
			if err != nil {
				logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
					ltsvlog.LV{"worker_id", workerID},
					ltsvlog.LV{"err", err})
				goto retryConnect
			}
			switch msgType {
			case msg.JobMsg:
				var job msg.Job
				err := msg.DecodeJob(dec, &job)
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

				jobResult := msg.JobResult{
					WorkerID: workerID,
					Results:  make([]msg.TargetResult, len(job.Targets)),
				}
				for i, target := range job.Targets {
					jobResult.Results[i] = msg.TargetResult{
						Target: target,
						Result: "success",
					}
				}
				err = msg.EncodeTypeAndJobResult(enc, &jobResult)
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
	if logger.DebugEnabled() {
		logger.Debug(ltsvlog.LV{"msg", "client connected to the server"},
			ltsvlog.LV{"address", address})
	}

	dec := msgpack.NewDecoder(conn)
	enc := msgpack.NewEncoder(conn)

	job := &msg.Job{
		Targets: []string{"target1", "target2"},
	}
	err = msg.EncodeTypeAndJob(enc, job)
	if err != nil {
		logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to write Job"},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
	logger.Info(ltsvlog.LV{"msg", "client sent Job"},
		ltsvlog.LV{"job", *job})

	msgType, err := msg.DecodeMessageType(dec)
	if err != nil {
		logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to read MessageType"},
			ltsvlog.LV{"err", err})
		os.Exit(1)
	}
	switch msgType {
	case msg.JobResultsMsg:
		var jobResults msg.JobResults
		err := msg.DecodeJobResults(dec, &jobResults)
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

package main

import (
	"io"

	"github.com/hnakamur/protobufio"
)

type messageReadWriter struct {
	*protobufio.MessageReader
	*protobufio.MessageWriter
	buf []byte
}

func newMessageReadWriter(rw io.ReadWriter) *messageReadWriter {
	return &messageReadWriter{
		MessageReader: protobufio.NewMessageReader(rw),
		MessageWriter: protobufio.NewMessageWriter(rw),
	}
}

func (rw *messageReadWriter) ReadMessageType() (MessageType, error) {
	v, _, err := rw.ReadVarint()
	if err != nil {
		return MessageType_UndefinedMsg, err
	}
	return MessageType(v), nil
}

func (rw *messageReadWriter) ReadRegisterWorker() (*RegisterWorker, error) {
	registerWorker := new(RegisterWorker)
	var err error
	rw.buf, _, _, err = rw.ReadVarintLenAndMessage(registerWorker, rw.buf)
	if err != nil {
		return nil, err
	}
	return registerWorker, nil
}

func (rw *messageReadWriter) ReadJob() (*Job, error) {
	job := new(Job)
	var err error
	rw.buf, _, _, err = rw.ReadVarintLenAndMessage(job, rw.buf)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (rw *messageReadWriter) ReadJobResult() (*JobResult, error) {
	jobResult := new(JobResult)
	var err error
	rw.buf, _, _, err = rw.ReadVarintLenAndMessage(jobResult, rw.buf)
	if err != nil {
		return nil, err
	}
	return jobResult, nil
}

func (rw *messageReadWriter) ReadJobResults() (*JobResults, error) {
	jobResults := new(JobResults)
	var err error
	rw.buf, _, _, err = rw.ReadVarintLenAndMessage(jobResults, rw.buf)
	if err != nil {
		return nil, err
	}
	return jobResults, nil
}

func (rw *messageReadWriter) writeMessageType(t MessageType) error {
	_, err := rw.WriteVarint(int64(t))
	return err
}

func (rw *messageReadWriter) WriteTypeAndRegsiterWorker(registerWorker *RegisterWorker) error {
	err := rw.writeMessageType(MessageType_RegisterWorkerMsg)
	if err != nil {
		return err
	}
	_, _, err = rw.WriteVarintLenAndMessage(registerWorker)
	return err
}

func (rw *messageReadWriter) WriteTypeAndJob(job *Job) error {
	err := rw.writeMessageType(MessageType_JobMsg)
	if err != nil {
		return err
	}
	_, _, err = rw.WriteVarintLenAndMessage(job)
	return err
}

func (rw *messageReadWriter) WriteTypeAndJobResult(jobResult *JobResult) error {
	err := rw.writeMessageType(MessageType_JobResultMsg)
	if err != nil {
		return err
	}
	_, _, err = rw.WriteVarintLenAndMessage(jobResult)
	return err
}

func (rw *messageReadWriter) WriteTypeAndJobResults(jobResults *JobResults) error {
	err := rw.writeMessageType(MessageType_JobResultsMsg)
	if err != nil {
		return err
	}
	_, _, err = rw.WriteVarintLenAndMessage(jobResults)
	return err
}

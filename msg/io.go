package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type MessageType uint32

const (
	UndefinedMsg      MessageType = 0
	JobMsg            MessageType = 1
	JobResultMsg      MessageType = 2
	JobResultsMsg     MessageType = 3
	RegisterWorkerMsg MessageType = 4
)

func EncodeTypeAndJob(e *msgpack.Encoder, j *Job) error {
	err := e.EncodeUint32(uint32(JobMsg))
	if err != nil {
		return err
	}
	return e.Encode(j)
}

func EncodeTypeAndJobResult(e *msgpack.Encoder, r *JobResult) error {
	err := e.EncodeUint32(uint32(JobResultMsg))
	if err != nil {
		return err
	}
	return e.Encode(r)
}

func EncodeTypeAndJobResults(e *msgpack.Encoder, r *JobResults) error {
	err := e.EncodeUint32(uint32(JobResultsMsg))
	if err != nil {
		return err
	}
	return e.Encode(r)
}

func EncodeTypeAndRegisterWorker(e *msgpack.Encoder, r *RegisterWorker) error {
	err := e.EncodeUint32(uint32(RegisterWorkerMsg))
	if err != nil {
		return err
	}
	return e.Encode(r)
}

func DecodeMessageType(d *msgpack.Decoder) (MessageType, error) {
	t, err := d.DecodeUint32()
	if err != nil {
		return UndefinedMsg, err
	}
	return MessageType(t), nil
}

func DecodeJob(d *msgpack.Decoder, j *Job) error {
	return d.Decode(j)
}

func DecodeJobResult(d *msgpack.Decoder, r *JobResult) error {
	return d.Decode(r)
}

func DecodeJobResults(d *msgpack.Decoder, r *JobResults) error {
	return d.Decode(r)
}

func DecodeRegisterWorker(d *msgpack.Decoder, r *RegisterWorker) error {
	return d.Decode(r)
}

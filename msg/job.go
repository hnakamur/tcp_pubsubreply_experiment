package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type Job struct {
	Targets []string
}

var (
	_ msgpack.CustomEncoder = &Job{}
	_ msgpack.CustomDecoder = &Job{}
)

func (j *Job) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(j.Targets)
}

func (j *Job) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&j.Targets)
}

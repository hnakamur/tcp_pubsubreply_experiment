package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type JobResults struct {
	Results []JobResult
}

var (
	_ msgpack.CustomEncoder = &JobResults{}
	_ msgpack.CustomDecoder = &JobResults{}
)

func (r *JobResults) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.Results)
}

func (r *JobResults) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.Results)
}

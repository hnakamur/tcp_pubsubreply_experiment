package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type JobResult struct {
	WorkerID string
	Results  []TargetResult
}

var (
	_ msgpack.CustomEncoder = &JobResult{}
	_ msgpack.CustomDecoder = &JobResult{}
)

func (r *JobResult) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.WorkerID, r.Results)
}

func (r *JobResult) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.WorkerID, &r.Results)
}

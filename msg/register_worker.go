package msg

import "gopkg.in/vmihailenco/msgpack.v2"

type RegisterWorker struct {
	WorkerID string
}

var (
	_ msgpack.CustomEncoder = &RegisterWorker{}
	_ msgpack.CustomDecoder = &RegisterWorker{}
)

func (r *RegisterWorker) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(r.WorkerID)
}

func (r *RegisterWorker) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&r.WorkerID)
}

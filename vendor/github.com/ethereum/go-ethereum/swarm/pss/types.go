package pss

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/swarm/storage"
	whisper "github.com/ethereum/go-ethereum/whisper/whisperv5"
)

const (
	defaultWhisperTTL = 6000
)

var (
	topicHashFunc = storage.MakeHashFunc("SHA256")()
)

type Topic whisper.TopicType

func (t *Topic) Unmarshal(input []byte) error {
	err := hexutil.UnmarshalFixedText("Topic", input, t[:])
	return err
}

// variable length address
type PssAddress []byte

type pssDigest [digestLength]byte

// Encapsulates messages transported over pss.
type PssMsg struct {
	To      []byte
	Expire  uint32
	Payload *whisper.Envelope
}

// serializes the message for use in cache
func (msg *PssMsg) serialize() []byte {
	rlpdata, _ := rlp.EncodeToBytes(msg)
	return rlpdata
}

// String representation of PssMsg
func (self *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", common.ToHex(self.To))
}

// Signature for a message handler function for a PssMsg
//
// Implementations of this type are passed to Pss.Register together with a topic,
type Handler func(msg []byte, p *p2p.Peer, asymmetric bool, keyid string) error

type stateStore struct {
	values map[string][]byte
}

func newStateStore() *stateStore {
	return &stateStore{values: make(map[string][]byte)}
}

func (store *stateStore) Load(key string) ([]byte, error) {
	return nil, nil
}

func (store *stateStore) Save(key string, v []byte) error {
	return nil
}

func BytesToTopic(b []byte) Topic {
	topicHashFunc.Reset()
	topicHashFunc.Write(b)
	return Topic(whisper.BytesToTopic(topicHashFunc.Sum(nil)))
}

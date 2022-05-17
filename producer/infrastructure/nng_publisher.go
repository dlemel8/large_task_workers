package infrastructure

import (
	"context"

	"github.com/pkg/errors"
	"go.nanomsg.org/mangos/v3/protocol"
	"go.nanomsg.org/mangos/v3/protocol/push"
	_ "go.nanomsg.org/mangos/v3/transport/all" // register transports
)

type NanoMsgNgPublisher struct {
	socket protocol.Socket
}

func NewNanoMsgPublisher(url string, queueMaxSize int64) (*NanoMsgNgPublisher, error) {
	socket, err := push.NewSocket()
	if err != nil {
		return nil, errors.Wrap(err, "can't get new push socket")
	}

	//if err := socket.SetOption(protocol.OptionWriteQLen, queueMaxSize); err != nil {
	//	return nil, errors.Wrap(err, "can't set write queue size on push socket")
	//}

	err = socket.Listen(url)
	if err != nil {
		return nil, errors.Wrap(err, "can't listen on push socket")
	}

	return &NanoMsgNgPublisher{socket: socket}, nil
}

func (n *NanoMsgNgPublisher) Publish(_ context.Context, bytes []byte) error {
	return n.socket.Send(bytes)
}

func (n *NanoMsgNgPublisher) Close() error {
	return n.Close()
}

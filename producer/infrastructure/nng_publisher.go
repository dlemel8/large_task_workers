package infrastructure

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.nanomsg.org/mangos/v3/protocol"
	"go.nanomsg.org/mangos/v3/protocol/push"
	_ "go.nanomsg.org/mangos/v3/transport/all" // register transports
)

const discoveryInterval = 10 * time.Second

type tcpEndpoint struct {
	name string
	port uint16
}

func (e tcpEndpoint) String() string {
	return fmt.Sprintf("tcp://%s:%d", e.name, e.port)
}

type NanoMsgNgPublisher struct {
	consumerEndpoint       string
	tcpPort                uint16
	queueMaxSize           int64
	consumersIpToSocket    map[string]protocol.Socket
	lastConsumersDiscovery time.Time
}

func NewNanoMsgPublisher(consumerEndpoint string, tcpPort uint16, queueMaxSize int64) (*NanoMsgNgPublisher, error) {
	return &NanoMsgNgPublisher{
		consumerEndpoint:    consumerEndpoint,
		tcpPort:             tcpPort,
		queueMaxSize:        queueMaxSize,
		consumersIpToSocket: make(map[string]protocol.Socket),
	}, nil
}

func (p *NanoMsgNgPublisher) Publish(_ context.Context, bytes []byte) error {
	now := time.Now()
	if p.lastConsumersDiscovery.Add(discoveryInterval).Before(now) {
		p.discoverConsumers()
		p.lastConsumersDiscovery = now
	}

	consumer, err := p.selectRandomConsumer()
	if err != nil {
		return err
	}

	return consumer.Send(bytes)
}

func (p *NanoMsgNgPublisher) Close() error {
	errorMessage := ""
	for ip, socket := range p.consumersIpToSocket {
		if err := socket.Close(); err != nil {
			errorMessage += fmt.Sprintf("failed to close socket %s: %s", ip, err)
		}
	}

	if errorMessage == "" {
		return nil
	}

	return errors.New(errorMessage)
}

func (p *NanoMsgNgPublisher) discoverConsumers() {
	log.WithField("endpoint", p.consumerEndpoint).Info("start consumers discovery")
	ips, err := net.LookupIP(p.consumerEndpoint)
	if err != nil {
		log.WithError(err).Error("failed to resolve consumer endpoint")
		return
	}

	verifiedIps := make(map[string]interface{})
	for _, ipObject := range ips {
		ip := ipObject.String()
		if _, ok := p.consumersIpToSocket[ip]; !ok {
			socket, err := p.createNewSocket(ip)
			if err != nil {
				log.WithFields(log.Fields{log.ErrorKey: err, "ip": ip}).Error("failed to create socket")
			} else {
				p.consumersIpToSocket[ip] = socket
			}
		}
		verifiedIps[ip] = nil
	}

	for ip, socket := range p.consumersIpToSocket {
		if _, ok := verifiedIps[ip]; ok {
			continue
		}

		if err := socket.Close(); err != nil {
			log.WithFields(log.Fields{log.ErrorKey: err, "ip": ip}).Error("failed to close socket")
		}
		delete(p.consumersIpToSocket, ip)
	}
	log.WithField("consumers", len(p.consumersIpToSocket)).Info("done consumers discovery")
}

func (p *NanoMsgNgPublisher) createNewSocket(ip string) (protocol.Socket, error) {
	socket, err := push.NewSocket()
	if err != nil {
		return nil, errors.Wrap(err, "can't get new push socket")
	}

	if err := socket.SetOption(protocol.OptionWriteQLen, int(p.queueMaxSize)); err != nil {
		return nil, errors.Wrap(err, "can't set write queue size on push socket")
	}

	err = socket.Dial(tcpEndpoint{name: ip, port: p.tcpPort}.String())
	if err != nil {
		return nil, errors.Wrap(err, "can't listen on push socket")
	}

	return socket, nil
}

func (p *NanoMsgNgPublisher) selectRandomConsumer() (protocol.Socket, error) {
	consumersCount := len(p.consumersIpToSocket)
	if consumersCount == 0 {
		return nil, errors.New("no consumers")
	}

	index := rand.Intn(consumersCount)
	for _, socket := range p.consumersIpToSocket {
		if index == 0 {
			return socket, nil
		}
		index -= 1
	}
	return nil, errors.New("failed to select a consumer")
}

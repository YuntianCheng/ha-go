package ha

import (
	"context"
	"github.com/sirupsen/logrus"
	"net"
)

const MAXREADSIZE = 100

type MulticastHandler func(size int, addr *net.UDPAddr, data []byte)

func (m *manager) listenMultiCast(ctx context.Context, handler MulticastHandler) {
	err := m.conn.SetReadBuffer(MAXREADSIZE)
	if err != nil {
		return
	}
	defer func() {
		logrus.Info("quit listening")
	}()
	go func() {
		defer func() {
			_ = m.conn.Close()
		}()
		for {
			b := make([]byte, MAXREADSIZE)
			num, addr, err := m.conn.ReadFromUDP(b)
			if err != nil {
				logrus.Error(err.Error())
				continue
			}
			go handler(num, addr, b)
		}
	}()
	<-ctx.Done()
}

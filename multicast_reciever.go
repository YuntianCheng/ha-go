package ha

import (
	"github.com/sirupsen/logrus"
	"net"
)

const MAXREADSIZE = 100

type MulticastHandler func(size int, addr *net.UDPAddr, data []byte)

func (m *manager) listenMultiCast(handler MulticastHandler) {
	err := m.conn.SetReadBuffer(MAXREADSIZE)
	if err != nil {
		return
	}
	for {
		b := make([]byte, MAXREADSIZE)
		num, addr, err := m.conn.ReadFromUDP(b)
		if err != nil {
			logrus.Error(err.Error())
			continue
		}
		go handler(num, addr, b)
	}
}

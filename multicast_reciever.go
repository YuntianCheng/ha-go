package ha

import (
	"net"
	"time"

	"github.com/sirupsen/logrus"
)

const MAXREADSIZE = 100

type MulticastHandler func(size int, addr *net.UDPAddr, data []byte, timer *time.Timer)

func (n *node) listenMultiCast(handler MulticastHandler) {
	n.conn.SetReadBuffer(MAXREADSIZE)
	for {
		b := make([]byte, MAXREADSIZE)
		num, addr, err := n.conn.ReadFromUDP(b)
		if err != nil {
			logrus.Error(err.Error())
			continue
		}
		go handler(num, addr, b, n.timeout)
	}
}

package ha

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

const MAXREADSIZE = 100

type MulticastHandler func(size int, addr *net.UDPAddr, data []byte)

func (m *manager) readMultiCastData(ctx context.Context, handler MulticastHandler) (err error) {
	err = m.conn.SetReadBuffer(MAXREADSIZE)
	if err != nil {
		return
	}
	defer func() {
		logrus.Info("quit reading")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			b := make([]byte, MAXREADSIZE)
			err = m.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				return
			}
			num, addr, err := m.conn.ReadFromUDP(b)
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				logrus.Error(err.Error())
				continue
			}
			go handler(num, addr, b)
		}
	}
}

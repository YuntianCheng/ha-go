package ha

import (
	"encoding/json"
	"ha/models"
	"net"

	"github.com/sirupsen/logrus"
)

func (n *node) sendHeatbeat(heatbeat models.Heartbeat) (err error) {
	data, err := json.Marshal(&heatbeat)
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	addr := n.groupIp + ":" + n.groupPort
	uAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	_, err = n.conn.WriteToUDP(data, uAddr)
	if err != nil {
		logrus.Error(err.Error())
	}
	return
}

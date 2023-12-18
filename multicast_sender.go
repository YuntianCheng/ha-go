package ha

import (
	"encoding/json"
	"ha/models"
	"net"

	"github.com/sirupsen/logrus"
)

func (m *manager) sendUniCastHeartbeat(heatbeat models.Heartbeat, addr string) (err error) {
	data, err := json.Marshal(&heatbeat)
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	uAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		//logrus.Error(err.Error())
		return
	}
	_, err = m.conn.WriteToUDP(data, uAddr)
	//if err != nil {
	//	logrus.Error(err.Error())
	//}
	return
}

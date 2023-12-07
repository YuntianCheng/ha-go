package ha

import (
	"encoding/json"
	"ha/models"
	"net"

	"github.com/sirupsen/logrus"
)

func (m *manager) sendHeatbeat(heatbeat models.Heartbeat) (err error) {
	data, err := json.Marshal(&heatbeat)
	if err != nil {
		logrus.Error(err.Error())
		return
	}
	addr := m.groupIp + ":" + m.groupPort
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

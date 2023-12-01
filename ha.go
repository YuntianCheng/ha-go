package ha

import (
	"context"
	"encoding/json"
	"errors"
	"ha/models"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type Node struct {
	m *manager
}

type manager struct {
	groupIp           string
	groupPort         string
	priority          int
	timeout           *time.Timer
	conn              *net.UDPConn
	rootContext       context.Context
	rootCancel        context.CancelFunc
	heatbeatFrequency time.Duration
	trigAfter         time.Duration
	candidateDuration time.Duration
	statelocker       sync.RWMutex
	beatCanncel       context.CancelFunc
	candiCanncel      context.CancelFunc
	jobCanncel        context.CancelFunc
	beatFlag          atomic.Bool
	jobFlag           atomic.Bool
	candidateFlag     atomic.Bool
	stateChan         chan int
	state             string
}

func NewNode(ctx context.Context, priority int, groupIp, groupPort, state string, hbFrequency, trigAfter, candidateDuration time.Duration) (n *Node, err error) {
	n.m = new(manager)
	n.m.rootContext, n.m.rootCancel = context.WithCancel(ctx)
	n.m.groupPort = groupPort
	n.m.groupIp = groupIp
	n.m.heatbeatFrequency = hbFrequency
	n.m.trigAfter = trigAfter
	n.m.candidateDuration = candidateDuration
	n.m.state = state
	n.m.priority = priority
	addr := n.m.groupIp + ":" + n.m.groupPort
	uAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return
	}
	n.m.conn, err = net.ListenMulticastUDP("udp", nil, uAddr)
	if err != nil {
		return
	}
	return
}

func (n *Node) Stop() {
	if n.m != nil {
		n.m.rootCancel()
	}
}

func (m *manager) heartBeatHandler(data []byte) {
	var hb models.Heartbeat
	err := json.Unmarshal(data, &hb)
	if err != nil {
		logrus.Error(err)
	}
	if m.checkMasterQualification(hb.Priority) {
		if m.state != "backup" {
			m.statelocker.Lock()
			m.state = "backup"
			m.statelocker.Unlock()
			m.stateChan <- 1
		}
	} else {
		if m.state == "backup" {
			m.statelocker.Lock()
			m.state = "candidate"
			m.statelocker.Unlock()
			m.stateChan <- 1
		}
	}
	if m.timeout.Stop() {
		m.timeout.Reset(2 * time.Second)
	}
}

func (m *manager) sendHeatBeatLoop(ctx context.Context) {
	heatbeatTicker := time.NewTicker(m.heatbeatFrequency)
	m.beatFlag.Store(true)
	defer func() {
		m.beatFlag.Store(false)
		heatbeatTicker.Stop()
	}()
	for {
		select {
		case <-heatbeatTicker.C:
			heatbeat := models.Heartbeat{
				Priority:  m.priority,
				Timestamp: time.Now().Unix(),
			}
			err := m.sendHeatbeat(heatbeat)
			if err != nil {
				logrus.Error(err.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *manager) checkMasterQualification(priority int) (result bool) {
	result = priority > m.priority
	return
}

func (m *manager) candidate(ctx context.Context) {
	m.candidateFlag.Store(true)
	timer := time.NewTimer(m.candidateDuration)
	select {
	case <-timer.C:
		m.statelocker.Lock()
		m.state = "master"
		m.statelocker.Unlock()
		m.candidateFlag.Store(false)
		m.stateChan <- 1
		return
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		m.candidateFlag.Store(false)
		return
	}
}

// 注册选举程序，如果在trigAfter时间内没收到心跳，选举程序就会触发
func (m *manager) registerCandidate() {
	trigFunc := func() {
		childCtx, canncel := context.WithCancel(m.rootContext)
		m.candiCanncel = canncel
		m.candidate(childCtx)
	}
	m.timeout = time.AfterFunc(m.trigAfter, trigFunc)
}

func (n *Node) Listen(custom MulticastHandler) (err error) {
	if n.m == nil {
		err = errors.New("can not listen on nil conn")
	}
	handler := func(size int, addr *net.UDPAddr, data []byte) {
		if custom != nil {
			custom(size, addr, data)
		}
		n.m.heartBeatHandler(data)
	}
	n.m.registerCandidate()
	n.m.listenMultiCast(handler)
	return
}

// 一次性Job用这个执行
func start(ctx context.Context, job func(context.Context)) {
	defer func() {
		if p := recover(); p != nil {
			logrus.Infof("Panic happened：%v", p)
		}
	}()
	job(ctx)
}

// 需要持续运行的Job调用这个执行，panic会自动重启Job，直到上下文被取消
func startWithRecovery(ctx context.Context, job func(context.Context), recoverTime time.Duration) {
	recoverer := func(childCtx context.Context, f func(context.Context)) {
		defer func() {
			if p := recover(); p != nil {
				logrus.Infof("Panic happened：%v, restart after %d seconds", p, recoverTime/time.Second)
			}
		}()
		f(childCtx)
	}
	watcherCtx, watcherCancal := context.WithCancel(ctx)
	defer watcherCancal()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			recoverer(watcherCtx, job)
		}
	}
}

func (m *manager) workStatus(job func(context.Context), recovery bool, duration time.Duration) {
	var state string
	m.statelocker.RLock()
	state = m.state
	m.statelocker.RUnlock()
	switch state {
	case "backup":
		if m.beatFlag.Load() {
			m.beatCanncel()
		}
		if m.jobFlag.Load() {
			m.jobCanncel()
		}
		if m.candidateFlag.Load() {
			m.candiCanncel()
		}
		m.timeout.Stop()
		m.timeout.Reset(2 * time.Second)
	case "master":
		if !m.beatFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.beatCanncel = cancel
			go m.sendHeatBeatLoop(ctx)

		}
		if !m.jobFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.jobCanncel = cancel
			if recovery {
				go startWithRecovery(ctx, job, duration)
			} else {
				go start(ctx, job)
			}
		}
	case "candidate":
		if !m.beatFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.candiCanncel = cancel
			go m.sendHeatBeatLoop(ctx)

		}
	default:
		logrus.Error("unrecognizable state")
	}
}

func (n *Node) WatchAndRun(job func(context.Context), recovery bool, duration time.Duration) (err error) {
	if n.m == nil {
		err = errors.New("can not start on a nil node")
	}
	defer func() {
		if n.m.timeout != nil {
			n.m.timeout.Stop()
		}
	}()
	n.m.timeout.Stop()
	n.m.timeout.Reset(n.m.trigAfter)
	var state string
	n.m.statelocker.RLock()
	state = n.m.state
	n.m.statelocker.RUnlock()
	if state == "master" {
		jctx, jCanncel := context.WithCancel(n.m.rootContext)
		bctx, bCanncel := context.WithCancel(n.m.rootContext)
		n.m.jobCanncel = jCanncel
		n.m.beatCanncel = bCanncel
		go n.m.sendHeatBeatLoop(bctx)
		go start(jctx, job)
	}
	for {
		select {
		case <-n.m.rootContext.Done():
			return
		case <-n.m.stateChan:
			n.m.workStatus(job, recovery, duration)
		}
	}
}

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

var (
	MASTER    = "master"
	CANDIDATE = "candidate"
	BACKUP    = "backup"
)

type Node struct {
	m *manager
}

type manager struct {
	groupIp           string
	groupPort         string
	priority          int
	timeout           *time.Timer
	timeoutLocker     sync.Mutex
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
	n = &Node{
		m: &manager{
			groupIp:           groupIp,
			groupPort:         groupPort,
			heatbeatFrequency: hbFrequency,
			trigAfter:         trigAfter,
			candidateDuration: candidateDuration,
			state:             state,
			priority:          priority,
			stateChan:         make(chan int),
		},
	}
	n.m.rootContext, n.m.rootCancel = context.WithCancel(ctx)
	addr := n.m.groupIp + ":" + n.m.groupPort
	uAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		n = nil
		return
	}
	n.m.conn, err = net.ListenMulticastUDP("udp", nil, uAddr)
	if err != nil {
		n = nil
	}
	return
}

func (n *Node) Stop() {
	if n.m != nil {
		n.m.rootCancel()
	}
}

func (m *manager) heartBeatHandler(n int, data []byte) {
	var hb models.Heartbeat
	err := json.Unmarshal(data[:n], &hb)
	if err != nil {
		logrus.Error(err)
	}
	logrus.Infof("received heartbeat, priority is %v, time is %v", hb.Priority, time.Unix(hb.Timestamp, 0))
	if m.checkMasterQualification(hb.Priority) {
		m.timeoutLocker.Lock()
		if m.timeout != nil && m.timeout.Stop() {
			m.timeout.Reset(2 * time.Second)
		}
		m.timeoutLocker.Unlock()
		if m.state != "backup" {
			logrus.Info("other node has higher priority, work as backup")
			m.statelocker.Lock()
			m.state = BACKUP
			m.statelocker.Unlock()
			m.stateChan <- 1
		}
	} else {
		if m.state == BACKUP {
			logrus.Info("master node has lower priority, start candidate")
			m.statelocker.Lock()
			m.state = CANDIDATE
			m.statelocker.Unlock()
			m.stateChan <- 1
		}
	}
}

func (m *manager) sendHeatBeatLoop(ctx context.Context) {
	heatbeatTicker := time.NewTicker(m.heatbeatFrequency)
	m.beatFlag.Store(true)
	defer func() {
		m.beatFlag.Store(false)
		heatbeatTicker.Stop()
	}()
	logrus.Info("start sending heartbeat")
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
			} else {
				logrus.Info("send heartbeat successful")
			}
		case <-ctx.Done():
			logrus.Info("stop sending heartbeat")
			return
		}
	}
}

func (m *manager) checkMasterQualification(priority int) (result bool) {
	result = priority > m.priority
	return
}

func (m *manager) candidate(ctx context.Context) {
	logrus.Info("start candidate")
	m.candidateFlag.Store(true)
	timer := time.NewTimer(m.candidateDuration)
	select {
	case <-timer.C:
		logrus.Info("win candidate, work as master")
		m.statelocker.Lock()
		m.state = MASTER
		m.statelocker.Unlock()
		m.candidateFlag.Store(false)
		m.stateChan <- 1
		return
	case <-ctx.Done():
		logrus.Info("cancel candidate, work as backup")
		if !timer.Stop() {
			<-timer.C
		}
		m.statelocker.Lock()
		m.state = BACKUP
		m.statelocker.Unlock()
		m.candidateFlag.Store(false)
		m.stateChan <- 1
		return
	}
}

// 注册选举程序，如果在trigAfter时间内没收到心跳，选举程序就会触发
func (m *manager) registerCandidate() {
	trigFunc := func() {
		var state string
		m.statelocker.RLock()
		state = m.state
		m.statelocker.RUnlock()
		if state == MASTER {
			return
		} else if state == BACKUP {
			m.statelocker.Lock()
			m.state = CANDIDATE
			m.statelocker.Unlock()
			m.stateChan <- 1
		}
	}
	m.timeoutLocker.Lock()
	m.timeout = time.AfterFunc(m.trigAfter, trigFunc)
	m.timeoutLocker.Unlock()
}

func (n *Node) Listen(custom MulticastHandler) (err error) {
	if n.m == nil {
		err = errors.New("can not listen on nil conn")
	}
	handler := func(size int, addr *net.UDPAddr, data []byte) {
		if custom != nil {
			custom(size, addr, data)
		}
		n.m.heartBeatHandler(size, data)
	}
	n.m.statelocker.RLock()
	if n.m.state == BACKUP {
		n.m.registerCandidate()
	}
	n.m.statelocker.RUnlock()
	ctx, cancel := context.WithCancel(n.m.rootContext)
	defer cancel()
	logrus.Info("start listening")
	n.m.listenMultiCast(ctx, handler)
	return
}

// 一次性Job用这个执行
func (m *manager) start(ctx context.Context, job func(context.Context)) {
	defer func() {
		if p := recover(); p != nil {
			logrus.Infof("Panic happened：%v", p)
		}
		logrus.Info("stop job")
		m.jobFlag.Store(false)
	}()
	m.jobFlag.Store(true)
	logrus.Info("start job")
	job(ctx)
}

// 需要持续运行的Job调用这个执行，panic会自动重启Job，直到上下文被取消
func (m *manager) startWithRecovery(ctx context.Context, job func(context.Context), recoverTime time.Duration) {
	recoverer := func(childCtx context.Context, f func(context.Context)) {
		defer func() {
			if p := recover(); p != nil {
				logrus.Infof("Panic happened：%v, restart after %d seconds", p, recoverTime/time.Second)
			}
		}()
		f(childCtx)
	}
	m.jobFlag.Store(true)
	watcherCtx, watcherCancel := context.WithCancel(ctx)
	defer func() {
		m.jobFlag.Store(false)
		watcherCancel()
		logrus.Info("stop job")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			logrus.Info("start job")
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
	case BACKUP:
		if m.beatFlag.Load() {
			m.beatCanncel()
		}
		if m.jobFlag.Load() {
			m.jobCanncel()
		}
		if m.candidateFlag.Load() {
			m.candiCanncel()
		}
		m.timeoutLocker.Lock()
		if m.timeout != nil {
			m.timeout.Stop()
		}
		m.timeoutLocker.Unlock()
		m.registerCandidate()
	case MASTER:
		if !m.beatFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.beatCanncel = cancel
			go m.sendHeatBeatLoop(ctx)

		}
		if !m.jobFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.jobCanncel = cancel
			if recovery {
				go m.startWithRecovery(ctx, job, duration)
			} else {
				go m.start(ctx, job)
			}
		}
		m.timeoutLocker.Lock()
		if m.timeout != nil {
			m.timeout.Stop()
		}
		m.timeoutLocker.Unlock()
	case CANDIDATE:
		if !m.beatFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.beatCanncel = cancel
			go m.sendHeatBeatLoop(ctx)
		}
		if !m.candidateFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.candiCanncel = cancel
			go m.candidate(ctx)
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
		n.m.timeoutLocker.Lock()
		if n.m.timeout != nil {
			n.m.timeout.Stop()
		}
		n.m.timeoutLocker.Unlock()
	}()
	n.m.timeoutLocker.Lock()
	if n.m.timeout != nil {
		n.m.timeout.Stop()
		n.m.timeout.Reset(n.m.trigAfter)
	}
	n.m.timeoutLocker.Unlock()
	var state string
	n.m.statelocker.RLock()
	state = n.m.state
	n.m.statelocker.RUnlock()
	if state == MASTER {
		jctx, jCanncel := context.WithCancel(n.m.rootContext)
		bctx, bCanncel := context.WithCancel(n.m.rootContext)
		n.m.jobCanncel = jCanncel
		n.m.beatCanncel = bCanncel
		go n.m.sendHeatBeatLoop(bctx)
		if recovery {
			go n.m.startWithRecovery(jctx, job, duration)
		} else {
			go n.m.start(jctx, job)
		}
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

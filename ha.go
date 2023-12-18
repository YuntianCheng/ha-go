package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	nodesLocker sync.RWMutex
	nodes       []string
	port        string

	priority int

	timeout       *time.Timer
	timeoutLocker sync.Mutex

	conn *net.UDPConn

	rootContext context.Context
	rootCancel  context.CancelFunc

	heartbeatFrequency time.Duration
	trigAfter          time.Duration
	candidateDuration  time.Duration

	stateLocker sync.RWMutex
	state       string
	stateChan   chan int

	beatCancel  context.CancelFunc
	candiCancel context.CancelFunc
	jobCancel   context.CancelFunc

	beatFlag      atomic.Bool
	jobFlag       atomic.Bool
	candidateFlag atomic.Bool
}

func NewUnicastNode(ctx context.Context, priority int, port, state string, hbFrequency, trigAfter, candidateDuration time.Duration) (n *Node, err error) {
	n = &Node{
		m: &manager{
			port:               port,
			heartbeatFrequency: hbFrequency,
			trigAfter:          trigAfter,
			candidateDuration:  candidateDuration,
			state:              state,
			priority:           priority,
			stateChan:          make(chan int),
		},
	}
	n.m.rootContext, n.m.rootCancel = context.WithCancel(ctx)
	uAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%s", n.m.port))
	if err != nil {
		n = nil
		return
	}
	n.m.conn, err = net.ListenUDP("udp", uAddr)
	if err != nil {
		n = nil
	}
	return
}

func (n *Node) Status() (status string) {
	n.m.stateLocker.RLock()
	status = n.m.state
	n.m.stateLocker.RUnlock()
	return
}

func (n *Node) Stop() {
	if n.m != nil {
		n.m.rootCancel()
		time.Sleep(1 * time.Second)
		_ = n.m.conn.Close()
	}
}

func (n *Node) AddFriend(node string) {
	n.m.nodesLocker.Lock()
	n.m.nodes = append(n.m.nodes, node)
	n.m.nodesLocker.Unlock()
}

func (n *Node) AddFriends(nodes []string) {
	n.m.nodesLocker.Lock()
	n.m.nodes = append(n.m.nodes, nodes...)
	n.m.nodesLocker.Unlock()
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
			m.stateLocker.Lock()
			m.state = BACKUP
			m.stateLocker.Unlock()
			m.stateChan <- 1
		}
	} else {
		if m.state == BACKUP {
			logrus.Info("master node has lower priority, start candidate")
			m.stateLocker.Lock()
			m.state = CANDIDATE
			m.stateLocker.Unlock()
			m.stateChan <- 1
		}
	}
}

func (m *manager) sendHeatBeatLoop(ctx context.Context) {
	heartbeatTicker := time.NewTicker(m.heartbeatFrequency)
	m.beatFlag.Store(true)
	defer func() {
		m.beatFlag.Store(false)
		heartbeatTicker.Stop()
	}()
	logrus.Info("start sending heartbeat")
	for {
		select {
		case <-heartbeatTicker.C:
			heartbeat := models.Heartbeat{
				Priority:  m.priority,
				Timestamp: time.Now().Unix(),
			}
			m.nodesLocker.RLock()
			for _, node := range m.nodes {
				err := m.sendUniCastHeartbeat(heartbeat, node)
				if err != nil {
					logrus.Errorf("send heartbeat to node %s faild, error is %s", node, err.Error())
				} else {
					logrus.Infof("send heartbeat to node %s successful", node)
				}
			}
			m.nodesLocker.RUnlock()
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
		m.stateLocker.Lock()
		m.state = MASTER
		m.stateLocker.Unlock()
		m.candidateFlag.Store(false)
		m.stateChan <- 1
		return
	case <-ctx.Done():
		logrus.Info("cancel candidate, work as backup")
		if !timer.Stop() {
			<-timer.C
		}
		m.stateLocker.Lock()
		m.state = BACKUP
		m.stateLocker.Unlock()
		m.candidateFlag.Store(false)
		m.stateChan <- 1
		return
	}
}

// 注册选举程序触发函数，如果在trigAfter时间内没收到心跳，选举程序就会触发
func (m *manager) registerCandidate() {
	trigFunc := func() {
		logrus.Info("heartbeat timeout, prepare to candidate")
		var state string
		m.stateLocker.RLock()
		state = m.state
		m.stateLocker.RUnlock()
		if state == MASTER {
			return
		} else if state == BACKUP {
			m.stateLocker.Lock()
			m.state = CANDIDATE
			m.stateLocker.Unlock()
			m.stateChan <- 1
		}
	}
	m.timeoutLocker.Lock()
	m.timeout = time.AfterFunc(m.trigAfter, trigFunc)
	m.timeoutLocker.Unlock()
}

func (n *Node) ReadHeartbeat(custom MulticastHandler) (err error) {
	if n.m == nil {
		err = errors.New("can not read on nil conn")
		return
	}
	handler := func(size int, addr *net.UDPAddr, data []byte) {
		if custom != nil {
			custom(size, addr, data)
		}
		n.m.heartBeatHandler(size, data)
	}
	n.m.stateLocker.RLock()
	if n.m.state == BACKUP {
		n.m.registerCandidate()
	}
	n.m.stateLocker.RUnlock()
	ctx, cancel := context.WithCancel(n.m.rootContext)
	logrus.Info("start reading heartbeat")
	defer cancel()
	err = n.m.readMultiCastData(ctx, handler)
	return
}

// 一次性Job用这个执行
func (m *manager) do(ctx context.Context, job func(context.Context)) {
	defer func() {
		if p := recover(); p != nil {
			logrus.Infof("Panic happened：%v", p)
		}
		logrus.Info("job stopped")
		m.jobFlag.Store(false)
	}()
	m.jobFlag.Store(true)
	logrus.Info("start job")
	job(ctx)
}

// 需要持续运行的Job调用这个执行，panic会自动重启Job，直到上下文被取消
func (m *manager) doWithRecovery(ctx context.Context, job func(context.Context), recoverTime time.Duration) {
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
		logrus.Info("job stopped")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			logrus.Info("start job")
			recoverer(watcherCtx, job)
		}
		logrus.Info("waiting")
		time.Sleep(recoverTime)
	}
}

func (m *manager) workStatus(job func(context.Context), recovery bool, duration time.Duration) {
	var state string
	m.stateLocker.RLock()
	state = m.state
	m.stateLocker.RUnlock()
	switch state {
	case BACKUP:
		if m.beatFlag.Load() {
			m.beatCancel()
		}
		if m.jobFlag.Load() {
			m.jobCancel()
		}
		if m.candidateFlag.Load() {
			m.candiCancel()
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
			m.beatCancel = cancel
			go m.sendHeatBeatLoop(ctx)

		}
		if !m.jobFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.jobCancel = cancel
			if recovery {
				go m.doWithRecovery(ctx, job, duration)
			} else {
				go m.do(ctx, job)
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
			m.beatCancel = cancel
			go m.sendHeatBeatLoop(ctx)
		}
		if !m.candidateFlag.Load() {
			ctx, cancel := context.WithCancel(m.rootContext)
			m.candiCancel = cancel
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
	n.m.stateLocker.RLock()
	state = n.m.state
	n.m.stateLocker.RUnlock()
	if state == MASTER {
		jctx, jCanncel := context.WithCancel(n.m.rootContext)
		bctx, bCanncel := context.WithCancel(n.m.rootContext)
		n.m.jobCancel = jCanncel
		n.m.beatCancel = bCanncel
		go n.m.sendHeatBeatLoop(bctx)
		if recovery {
			go n.m.doWithRecovery(jctx, job, duration)
		} else {
			go n.m.do(jctx, job)
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

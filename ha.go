package ha

import (
	"context"
	"encoding/json"
	"ha/models"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// var (
// 	statelocker   sync.RWMutex
// 	beatCanncel   context.CancelFunc
// 	candiCanncel  context.CancelFunc
// 	jobCanncel    context.CancelFunc
// 	beatFlag      atomic.Bool
// 	jobFlag       atomic.Bool
// 	candidateFlag atomic.Bool
// 	stateChan     = make(chan int, 1)
// )

type node struct {
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

func (n *node) heartBeatHandler(size int, addr *net.UDPAddr, data []byte, timer *time.Timer) {
	var hb models.Heartbeat
	err := json.Unmarshal(data, &hb)
	if err != nil {
		logrus.Error(err)
	}
	if n.checkMasterQualification(hb.Priority) {
		if n.state != "backup" {
			n.statelocker.Lock()
			n.state = "backup"
			n.statelocker.Unlock()
			n.stateChan <- 1
		}
	} else {
		if n.state == "backup" {
			n.statelocker.Lock()
			n.state = "candidate"
			n.statelocker.Unlock()
			n.stateChan <- 1
		}
	}
	if timer.Stop() {
		timer.Reset(2 * time.Second)
	}
}

func (n *node) sendHeatBeatLoop(ctx context.Context) {
	heatbeatTicker := time.NewTicker(n.heatbeatFrequency)
	n.beatFlag.Store(true)
	defer func() {
		n.beatFlag.Store(false)
		heatbeatTicker.Stop()
	}()
	for {
		select {
		case <-heatbeatTicker.C:
			heatbeat := models.Heartbeat{
				Priority:  n.priority,
				Timestamp: time.Now().Unix(),
			}
			err := n.sendHeatbeat(heatbeat)
			if err != nil {
				logrus.Error(err.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (n *node) checkMasterQualification(priority int) (result bool) {
	result = priority > n.priority
	return
}

func (n *node) candidate(ctx context.Context) {
	n.candidateFlag.Store(true)
	timer := time.NewTimer(n.candidateDuration)
	select {
	case <-timer.C:
		n.statelocker.Lock()
		n.state = "master"
		n.statelocker.Unlock()
		n.candidateFlag.Store(false)
		n.stateChan <- 1
		return
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		n.candidateFlag.Store(false)
		return
	}
}

// 注册选举程序，如果在trigAfter时间内没收到心跳，选举程序就会触发
func (n *node) registerCandidate() {
	trigFunc := func() {
		childCtx, canncel := context.WithCancel(n.rootContext)
		n.candiCanncel = canncel
		n.candidate(childCtx)
	}
	n.timeout = time.AfterFunc(n.trigAfter, trigFunc)
}

func (n *node) Listen() {
	n.registerCandidate()
	n.listenMultiCast(n.heartBeatHandler)
}

func (n *node) RegisterHeatbeatHandler(handler MulticastHandler) {

}

// 一次性Job用这个执行
func (n *node) start(ctx context.Context, job func(context.Context)) {
	defer func() {
		if p := recover(); p != nil {
			logrus.Infof("Panic happened：%v", p)
		}
	}()
	job(ctx)
}

// 需要持续运行的Job调用这个执行，panic会自动重启Job，直到上下文被取消
func (n *node) startWithRecovery(ctx context.Context, job func(context.Context), recoverTime time.Duration) {
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

func (n *node) WatchAndRun(c context.Context, timer *time.Timer, job func(context.Context)) {
	n.timeout.Stop()
	n.timeout.Reset(n.trigAfter)
	var state string
	n.statelocker.RLock()
	state = n.state
	n.statelocker.RUnlock()
	if state == "master" {
		jctx, jCanncel := context.WithCancel(c)
		bctx, bCanncel := context.WithCancel(c)
		n.jobCanncel = jCanncel
		n.beatCanncel = bCanncel
		go n.sendHeatBeatLoop(bctx)
		go n.start(jctx, job)
	}
	for range n.stateChan {
		n.statelocker.RLock()
		state = n.state
		n.statelocker.RUnlock()
		switch state {
		case "backup":
			if n.beatFlag.Load() {
				n.beatCanncel()
			}
			if n.jobFlag.Load() {
				n.jobCanncel()
			}
			if n.candidateFlag.Load() {
				n.candiCanncel()
			}
			if !timer.Stop() {
				timer.Reset(2 * time.Second)
			}
		case "master":
			if !n.beatFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.beatCanncel = cancel
				go n.sendHeatBeatLoop(ctx)

			}
			if !n.jobFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.jobCanncel = cancel
				go n.start(ctx, job)

			}
		case "candidate":
			if !n.beatFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.candiCanncel = cancel
				go n.sendHeatBeatLoop(ctx)

			}
		default:
			logrus.Error("unrecognizable state")
		}
	}
}

func (n *node) WatchAndRunWithRecovery(c context.Context, timer *time.Timer, duration time.Duration, job func(context.Context)) {
	var state string
	n.statelocker.RLock()
	state = n.state
	n.statelocker.RUnlock()
	if state == "master" {
		jctx, jCanncel := context.WithCancel(c)
		bctx, bCanncel := context.WithCancel(c)
		n.jobCanncel = jCanncel
		n.beatCanncel = bCanncel
		go n.sendHeatBeatLoop(bctx)
		go n.startWithRecovery(jctx, job, duration)
	}
	for range n.stateChan {
		n.statelocker.RLock()
		state = n.state
		n.statelocker.RUnlock()
		switch state {
		case "backup":
			if n.beatFlag.Load() {
				n.beatCanncel()
			}
			if n.jobFlag.Load() {
				n.jobCanncel()
			}
			if n.candidateFlag.Load() {
				n.candiCanncel()
			}
			if !timer.Stop() {
				timer.Reset(2 * time.Second)
			}
		case "master":
			if !n.beatFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.beatCanncel = cancel
				go n.sendHeatBeatLoop(ctx)

			}
			if !n.jobFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.jobCanncel = cancel
				go n.startWithRecovery(ctx, job, duration)

			}
		case "candidate":
			if !n.beatFlag.Load() {
				ctx, cancel := context.WithCancel(c)
				n.candiCanncel = cancel
				go n.sendHeatBeatLoop(ctx)

			}
		default:
			logrus.Error("unrecognizable state")
		}
	}
}

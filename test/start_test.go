package test

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"ha"
	"testing"
	"time"
)

func TestMaster(t *testing.T) {
	node, err := ha.NewNode(context.Background(), 100, "224.0.0.18", "2345", "001", "backup", 1*time.Second, 5*time.Second, 5*time.Second)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	go func() {
		err = node.ReadHeartbeat(nil)
		if err != nil {
			fmt.Println(err.Error())
			node.Stop()
		}
	}()
	job := func(ctx context.Context) {
		defer func() {
			logrus.Info("quit job")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				logrus.Info("doing my job")
			}
			time.Sleep(3 * time.Second)
		}
	}
	go func() {
		err = node.WatchAndRun(job, true, 0)
		if err != nil {
			fmt.Println(err.Error())
			node.Stop()
		}
	}()
	time.Sleep(300 * time.Second)
	node.Stop()
	time.Sleep(10 * time.Second)
}

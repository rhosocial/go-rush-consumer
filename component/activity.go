package component

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

var Activities *ActivityPool

type ActivityPool struct {
	Activities       map[uint64]*Activity
	ActivitiesRWLock sync.RWMutex
}

func InitActivityPool() *ActivityPool {
	a := &ActivityPool{
		Activities: make(map[uint64]*Activity),
	}
	return a
}

func (a *ActivityPool) Capacity() int {
	return len(a.Activities)
}

func (a *ActivityPool) New(id uint64) error {
	a.ActivitiesRWLock.Lock()
	defer a.ActivitiesRWLock.Unlock()
	if _, existed := a.Activities[id]; existed {
		return ErrActivityExisted
	}
	a.Activities[id] = &Activity{
		ID: id,
	}
	return nil
}

func (a *ActivityPool) ActivityExists(id uint64) bool {
	_, existed := a.Activities[id]
	return existed
}

func (a *ActivityPool) GetActivity(id uint64) (*Activity, error) {
	a.ActivitiesRWLock.Lock()
	defer a.ActivitiesRWLock.Unlock()
	activity, existed := a.Activities[id]
	if !existed {
		return nil, ErrActivityNotExist
	}
	return activity, nil
}

var ErrActivityNotExist = errors.New("activity not exist")
var ErrActivityExisted = errors.New("activity existed")

func (a *ActivityPool) Remove(id uint64, stopBeforeRemoving bool) error {
	a.ActivitiesRWLock.Lock()
	defer a.ActivitiesRWLock.Unlock()
	activity, existed := a.Activities[id]
	if !existed {
		return ErrActivityNotExist
	}
	if activity.IsWorking() {
		if !stopBeforeRemoving {
			return ErrWorkerIsWorking
		}
		err := activity.Stop()
		if err != nil {
			return err
		}
	}
	delete(a.Activities, id)
	return nil
}

func (a *ActivityPool) Status() map[uint64]bool {
	workings := make(map[uint64]bool)
	for _, v := range a.Activities {
		workings[v.ID] = v.ContextCancelFunc != nil
	}
	return workings
}

func (a *ActivityPool) StopAll() int {
	count := 0
	for _, v := range a.Activities {
		err := v.Stop()
		if err == nil {
			count++
		}
	}
	return count
}

type Activity struct {
	ID                      uint64
	ContextCancelFuncRWLock sync.RWMutex
	ContextCancelFunc       context.CancelFunc
}

type RedisConnection struct {
	Addr     string
	Username string
	Password string
	DB       int
}

func (c *Activity) GetRedisServerApplicationKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Application, c.ID)
}

func (c *Activity) GetRedisServerApplicantKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Applicant, c.ID)
}

func (c *Activity) GetRedisServerSeatKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Seat, c.ID)
}

var ErrWorkerHasBeenStopped = errors.New("the worker has already been stopped")
var ErrWorkerIsWorking = errors.New("the worker is working")

func (c *Activity) Start(ctx context.Context) error {
	turn := commonComponent.GlobalRedisClientPool.GetCurrentTurn()
	if turn == nil {
		panic(commonComponent.ErrRedisClientNil)
	}
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	if c.ContextCancelFunc != nil {
		return ErrWorkerIsWorking
	}
	ctxChild, cancel := context.WithCancel(ctx)
	c.ContextCancelFunc = cancel
	server := GlobalEnv.RedisServers[*turn]
	interval := server.GetWorkerDefault().Interval
	if server.Worker != nil {
		interval = server.Worker.Interval
	}
	go worker(ctxChild, interval, c.ID, processFunc, doneFunc)
	return nil
}

var processFunc = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	if err != nil {
		panic(err)
	}
	results := activity.PopApplicationsFromQueue(ctx)
	output := "<empty set>"
	if len(results) > 0 {
		output = strings.Join(results, ", ")
		count := activity.PushApplicationsIntoSeatQueue(ctx, results)
		log.Printf("[ActivityID: %d] %d application(s) accepted.\n", activityID, count)
	}
	log.Printf("[ActivityID: %d] Results: %s", activityID, output)
}

func (c *Activity) Stop() error {
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	if c.ContextCancelFunc == nil {
		return ErrWorkerHasBeenStopped
	}
	c.ContextCancelFunc()
	c.ContextCancelFunc = nil
	return nil
}

func (c *Activity) IsWorking() bool {
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	return c.ContextCancelFunc != nil
}

var currentClient func() *redis.Client

func (c *Activity) PopApplicationsFromQueue(ctx context.Context) []string {
	batch := int(GlobalEnv.Activity.Batch)
	client := currentClient()
	if result := client.LLen(ctx, c.GetRedisServerApplicationKeyName()); result.Err() != nil {
		panic(result.Err())
	} else {
		batch = int(math.Min(float64(result.Val()), float64(batch)))
	}
	values := client.LPopCount(ctx, c.GetRedisServerApplicationKeyName(), batch)
	return values.Val()
}

func (c *Activity) PushApplicationsIntoSeatQueue(ctx context.Context, applications []string) int64 {
	if len(applications) == 0 {
		return 0
	}
	count := int64(0)
	for _, value := range applications {
		if !c.ApplicationExists(ctx, value) {
			continue
		}
		tm := time.Now().UnixMicro()
		result := currentClient().ZAddNX(ctx, c.GetRedisServerSeatKeyName(), redis.Z{
			Score:  float64(tm),
			Member: c.GetApplicant(ctx, value),
		})
		if result.Err() == nil {
			count += result.Val()
		} else {
			println(result.Err())
		}
	}
	return count
}

func (c *Activity) ApplicationExists(ctx context.Context, application string) bool {
	if len(application) == 0 {
		return false
	}
	if result := currentClient().HExists(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return false
}

func (c *Activity) GetApplicant(ctx context.Context, application string) string {
	if result := currentClient().HGet(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return ""
}

func (c *Activity) GetSeatCount(ctx context.Context) int64 {
	if result := currentClient().ZCount(ctx, c.GetRedisServerSeatKeyName(), "-inf", "+inf"); result.Err() == nil {
		return result.Val()
	}
	return 0
}

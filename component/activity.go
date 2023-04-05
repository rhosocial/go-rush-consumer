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

// ActivityPool 表示活动池。
type ActivityPool struct {
	Activities       map[uint64]*Activity // 活动映射。key 为活动ID。
	ActivitiesRWLock sync.RWMutex         // 操纵活动的锁。
}

// InitActivityPool 初始化活动池。
// 用法：
// Activities = InitActivityPool()
// 请勿直接声明活动池变量，除非你知道这么做的后果。
func InitActivityPool() *ActivityPool {
	a := &ActivityPool{
		Activities: make(map[uint64]*Activity),
	}
	return a
}

// Capacity 返回活动数量。
func (a *ActivityPool) Capacity() int {
	return len(a.Activities)
}

// New 新增一个活动。需要指定活动ID。
// id 活动ID。新增的活动ID不能与已存在的ID相同，否则会报 ErrActivityExisted 异常。
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

// GetActivity 返回指定活动ID的活动指针。若指定ID的活动不存在，则返回空指针和 ErrActivityNotExist 异常。
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

// Remove 移除指定活动ID。
// 若指定ID的活动不存在，则报 ErrActivityNotExist 异常。
// 若 stopBeforeRemoving == false 且活动正在工作中时，则报 ErrWorkerIsWorking 异常。
// 若未报任何异常，则表示移除成功。
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

// StopAll 停止所用活动的工作协程。返回成功停止的活动数。
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

// Activity 活动
type Activity struct {
	ID                      uint64
	ContextCancelFuncRWLock sync.RWMutex
	ContextCancelFunc       context.CancelFunc
}

func (c *Activity) GetRedisServerApplicationKeyName() string {
	return fmt.Sprintf("%s%d", (*GlobalEnv).Activity.RedisServer.KeyPrefix.Application, c.ID)
}

func (c *Activity) GetRedisServerApplicantKeyName() string {
	return fmt.Sprintf("%s%d", (*GlobalEnv).Activity.RedisServer.KeyPrefix.Applicant, c.ID)
}

func (c *Activity) GetRedisServerSeatKeyName() string {
	return fmt.Sprintf("%s%d", (*GlobalEnv).Activity.RedisServer.KeyPrefix.Seat, c.ID)
}

var ErrWorkerHasBeenStopped = errors.New("the worker has already been stopped")
var ErrWorkerIsWorking = errors.New("the worker is working")

// Start 启动一个活动的工作协程。若启动成功，则不会报异常且返回 nil。
// 若当前无有效 Redis 客户端，则报 ErrRedisClientNil 异常。
// 若当前活动已经启动了工作协程，则返回 ErrWorkerIsWorking 异常。
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
	server := (*GlobalEnv).RedisServers[*turn]
	interval := server.GetWorkerDefault().Interval
	if server.Worker != nil {
		interval = server.Worker.Interval
	}
	go worker(ctxChild, interval, c.ID, processFunc, doneFunc)
	return nil
}

// processFunc 工作协程。
// 从申请队列中取出一批，再送入席位
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

// Stop 停止一个活动的工作协程。若停止成功，则返回 nil。
// 若指定的活动的工作协程已停止，则会返回 ErrWorkerHasBeenStopped 异常。
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

// IsWorking 判断当前活动协程否正在工作中。
func (c *Activity) IsWorking() bool {
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	return c.ContextCancelFunc != nil
}

// currentClient 指代获取当前有效 Redis 客户端指针的方法。
var currentClient func() *redis.Client

// PopApplicationsFromQueue 从申请队列中取出。
func (c *Activity) PopApplicationsFromQueue(ctx context.Context) []string {
	batch := int((*GlobalEnv).Activity.Batch)
	client := currentClient()
	if result := client.LLen(ctx, c.GetRedisServerApplicationKeyName()); result.Err() != nil {
		panic(result.Err())
	} else {
		batch = int(math.Min(float64(result.Val()), float64(batch)))
	}
	values := client.LPopCount(ctx, c.GetRedisServerApplicationKeyName(), batch)
	return values.Val()
}

// PushApplicationsIntoSeatQueue 将"申请"送入"席位"中。返回值为成功送入的总数。
// "席位"采用有序表实现，分数为毫秒。存在不同"申请"分数相同的可能。如果指定的"申请"已存在，则不会更新。"申请"不存在时才会送入有序表中，并计数。
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
			log.Println(result.Err())
		}
	}
	return count
}

// ApplicationExists 判断"申请"是否存在。
func (c *Activity) ApplicationExists(ctx context.Context, application string) bool {
	if len(application) == 0 {
		return false
	}
	if result := currentClient().HExists(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return false
}

// GetApplicant 根据"申请"获取申请人。如果申请人不存在，则返回空字符串。
func (c *Activity) GetApplicant(ctx context.Context, application string) string {
	if result := currentClient().HGet(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return ""
}

// GetSeatCount 获取已确定"席位"的数量。
func (c *Activity) GetSeatCount(ctx context.Context) int64 {
	if result := currentClient().ZCount(ctx, c.GetRedisServerSeatKeyName(), "-inf", "+inf"); result.Err() == nil {
		return result.Val()
	}
	return 0
}

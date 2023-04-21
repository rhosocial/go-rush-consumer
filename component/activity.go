package component

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rhosocial/go-rush-common/component/environment"
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
	a.ActivitiesRWLock.RLock()
	defer a.ActivitiesRWLock.RUnlock()
	return len(a.Activities)
}

// New 新增一个活动。需要指定活动ID。
// id 活动ID。新增的活动ID不能与已存在的ID相同，否则会报 ErrActivityExisted 异常。
func (a *ActivityPool) New(id uint64, index *uint8) error {
	a.ActivitiesRWLock.Lock()
	defer a.ActivitiesRWLock.Unlock()
	if _, existed := a.Activities[id]; existed {
		return ErrActivityExisted
	}
	index0 := uint8(0)
	if index != nil {
		index0 = *index
	}
	a.Activities[id] = &Activity{
		ID:               id,
		RedisServerIndex: index0,
		Batch:            10000,
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
var ErrActivityToBeRemoved = errors.New("activity to be removed")
var ErrAllWorkersStopped = errors.New("all workers stopped")

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
		err := activity.Stop(ErrActivityToBeRemoved)
		if err != nil {
			return err
		}
	}
	delete(a.Activities, id)
	return nil
}

// RemoveAll 移除所有活动，并返回成功移除的个数。
func (a *ActivityPool) RemoveAll() int {
	a.ActivitiesRWLock.Lock()
	defer a.ActivitiesRWLock.Unlock()
	count := 0
	for _, v := range a.Activities {
		if v.IsWorking() {
			if err := v.Stop(ErrActivityToBeRemoved); err == nil {
				count++
			} else {
				log.Println(err)
			}
		}
		delete(a.Activities, v.ID)
	}
	return count
}

type ActivityStatus struct {
	IsWorking        bool  `json:"is_working"`
	RedisServerIndex uint8 `json:"redis_server_index"`
}

func (a *ActivityPool) Status() map[uint64]ActivityStatus {
	status := make(map[uint64]ActivityStatus)
	for _, v := range a.Activities {
		status[v.ID] = ActivityStatus{
			IsWorking:        v.IsWorking(),
			RedisServerIndex: v.RedisServerIndex,
		}
	}
	return status
}

// StopAll 停止所用活动的工作协程。返回成功停止的活动数。
func (a *ActivityPool) StopAll() int {
	if a == nil {
		return 0
	}
	count := 0
	for _, v := range a.Activities {
		err := v.Stop(ErrAllWorkersStopped)
		if err == nil {
			count++
		}
	}
	return count
}

// Activity 活动
type Activity struct {
	ID                      uint64
	RedisServerIndex        uint8  `json:"redis_server_index" default:"0"`
	Batch                   uint16 `json:"batch" default:"10000"`
	contextCancelFuncRWLock sync.RWMutex
	contextCancelFunc       context.CancelCauseFunc
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

// ErrWorkerStopped 任务成功停止。但停止要提供 errors 类原因，因此该成功状态仍记为异常。
var ErrWorkerStopped = errors.New("the worker stopped")

// Start 启动一个活动的工作协程。若启动成功，则不会报异常且返回 nil。
// 若当前无有效 Redis 客户端，则报 ErrRedisClientNil 异常。
// 若当前活动已经启动了工作协程，则返回 ErrWorkerIsWorking 异常。
func (c *Activity) Start(ctx context.Context) error {
	c.contextCancelFuncRWLock.Lock()
	defer c.contextCancelFuncRWLock.Unlock()
	if c.contextCancelFunc != nil {
		return ErrWorkerIsWorking
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	c.contextCancelFunc = cancel
	go worker(ctxChild, 1000, c.ID, processFunc3, nil)
	return nil
}

// processFunc3 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。该做法采用送入一次性执行的 lua 脚本而实现“取”和“存”的原子化，即不会因“席位”变化
// 中失败，也不会因为多个工作进程而导致乱序，又因一次性操作减少了与客户端交互的次数。因此推荐采用该做法。
var processFunc3 = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	client := environment.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err != nil {
		panic(err)
	}
	tmStart := time.Now()
	if val, err := client.FCall(ctx, "pop_applications_and_push_into_seats", []string{
		activity.GetRedisServerApplicationKeyName(),
		activity.GetRedisServerApplicantKeyName(),
		activity.GetRedisServerSeatKeyName(),
	}, activity.Batch).Uint64Slice(); err == nil {
		timeElapsed := time.Now().Sub(tmStart)
		if timeElapsed > time.Minute {
			timeElapsed = timeElapsed.Truncate(time.Second)
		}
		log.Printf("[ActivityID: %d]: %d application(s): %d seat(s) newly confirmed, %d skipped, %d applicant(s) missing, time elapsed : %13v.\n",
			activityID, val[0], val[1], val[2], val[3], timeElapsed)
	} else {
		log.Printf("[ActivityID: %d]: %s\n", activityID, err.Error())
		panic(err)
	}
}

// Stop 停止一个活动的工作协程。若停止成功，则返回 nil。
// 停止必须指定原因。如果为成功停止，则传入 ErrWorkerStopped。
// 若指定的活动的工作协程已停止，则会返回 ErrWorkerHasBeenStopped 异常。
func (c *Activity) Stop(cause error) error {
	c.contextCancelFuncRWLock.Lock()
	defer c.contextCancelFuncRWLock.Unlock()
	if c.contextCancelFunc == nil {
		return ErrWorkerHasBeenStopped
	}
	c.contextCancelFunc(cause)
	c.contextCancelFunc = nil
	return nil
}

// IsWorking 判断当前活动协程否正在工作中。
func (c *Activity) IsWorking() bool {
	c.contextCancelFuncRWLock.RLock()
	defer c.contextCancelFuncRWLock.RUnlock()
	return c.contextCancelFunc != nil
}

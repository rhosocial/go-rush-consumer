package component

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	commonComponent "github.com/rhosocial/go-rush-common/component"
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

func (a *ActivityPool) Status() map[uint64]bool {
	workings := make(map[uint64]bool)
	for _, v := range a.Activities {
		workings[v.ID] = v.contextCancelFunc != nil
	}
	return workings
}

// StopAll 停止所用活动的工作协程。返回成功停止的活动数。
func (a *ActivityPool) StopAll() int {
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
	RedisServerIndex        uint8
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
	turn := commonComponent.GlobalRedisClientPool.GetCurrentTurn()
	if turn == nil {
		panic(commonComponent.ErrRedisClientNil)
	}
	c.contextCancelFuncRWLock.Lock()
	defer c.contextCancelFuncRWLock.Unlock()
	if c.contextCancelFunc != nil {
		return ErrWorkerIsWorking
	}
	ctxChild, cancel := context.WithCancelCause(ctx)
	c.contextCancelFunc = cancel
	server := (*(*GlobalEnv).RedisServers)[*turn]
	interval := server.GetWorkerDefault().Interval
	if server.Worker != nil {
		interval = server.Worker.Interval
	}
	go worker(ctxChild, interval, c.ID, processFunc3, nil)
	return nil
}

// processFunc 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。但强烈不推荐此做法，除非对性能要求不高，且可以保证处理进程只有一个，否则，因“取”和
// “存”之间并非原子操作而导致乱序。此处仅做流程展示，并非用在实际应用中。
var processFunc = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	// 活动必须存在。
	if err != nil {
		panic(err)
	}
	// 从“申请”队列获取一批“申请”
	results := activity.PopApplicationsFromQueue(ctx)
	output := "<empty set>"
	// 如果有“申请”，则处理。否则按“空”。
	if len(results) > 0 {
		output = strings.Join(results, ", ")
		// 将“申请”对应的“申请人”送入“席位”中。已经存在的申请则忽略，未存在的按顺序依次列在最后。
		count := activity.PushApplicationsIntoSeatQueue(ctx, results)
		log.Printf("[ActivityID: %d] %d application(s) accepted.\n", activityID, count)
	}
	log.Printf("[ActivityID: %d] Results: %s", activityID, output)
}

// processFunc2 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。但依然不推荐此做法，除非可以保证处理进程只有一个，且处理期间“席位”没有发生修改，
// 否则，因“存”时频繁监控到“席位”有序表已发生修改而失败。
var processFunc2 = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err != nil {
		panic(err)
	}
	txf := func(tx *redis.Tx) error {
		// 取得一批的最大值。该值不推荐设太大，否则在高频存取时，可能会因乐观锁频繁失效而无法成功执行。
		batch := int(*(*(*GlobalEnv).Activity).Batch)
		if result := tx.LLen(ctx, activity.GetRedisServerApplicationKeyName()); result.Err() != nil {
			return result.Err()
		} else {
			batch = int(math.Min(float64(result.Val()), float64(batch)))
		}

		// 从“申请”队列获取一批“申请”
		applications := tx.LPopCount(ctx, activity.GetRedisServerApplicationKeyName(), batch).Val()
		output := "<empty set>"

		// 如果有“申请”，则处理。否则按“空”。
		if len(applications) > 0 {
			output = strings.Join(applications, ", ")
			// 将“申请”对应的“申请人”送入“席位”中。已经存在的申请则忽略，未存在的按顺序依次列在最后。
			count := int64(0)
			for _, application := range applications {
				// 如果“申请人”不存在，则忽略。
				if result := tx.HExists(ctx, activity.GetRedisServerApplicantKeyName(), application); result.Err() != nil || result.Val() == false {
					continue
				}
				// 取得“申请人”。
				applicantResult := client.HGet(ctx, activity.GetRedisServerApplicantKeyName(), application)
				if result := client.ZAddNX(ctx, activity.GetRedisServerSeatKeyName(), redis.Z{
					Score:  float64(time.Now().UnixMicro()),
					Member: applicantResult.Val(),
				}); result.Err() == nil {
					count += result.Val()
				} else {
					return result.Err()
				}
			}
			log.Printf("[ActivityID: %d] %d application(s) accepted.\n", activityID, count)
		}
		log.Printf("[ActivityID: %d] Results: %s", activityID, output)
		return nil
	}
	if err := client.Watch(ctx, txf, activity.GetRedisServerApplicantKeyName(), activity.GetRedisServerSeatKeyName(), activity.GetRedisServerApplicationKeyName()); err == redis.TxFailedErr {
		log.Printf("[ActivityID: %d]: %s\n", activityID, err.Error())
	} else if err != nil {
		log.Printf("[ActivityID: %d]: %s\n", activityID, err.Error())
	}
}

// processFunc3 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。该做法采用送入一次性执行的 lua 脚本而实现“取”和“存”的原子化，即不会因“席位”变化
// 中失败，也不会因为多个工作进程而导致乱序，又因一次性操作减少了与客户端交互的次数。因此推荐采用该做法。
var processFunc3 = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err != nil {
		panic(err)
	}
	if val, err := client.FCall(ctx, "pop_applications_and_push_into_seats", []string{
		activity.GetRedisServerApplicationKeyName(),
		activity.GetRedisServerApplicantKeyName(),
		activity.GetRedisServerSeatKeyName(),
	}, *(*(*GlobalEnv).Activity).Batch).Uint64Slice(); err == nil {
		log.Printf("[ActivityID: %d]: %d application(s): %d seat(s) newly confirmed, %d skipped, %d applicant(s) missing.\n",
			activityID, val[0], val[1], val[2], val[3])
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

// PopApplicationsFromQueue 从申请队列中取出。
func (c *Activity) PopApplicationsFromQueue(ctx context.Context) []string {
	batch := int(*(*(*GlobalEnv).Activity).Batch)
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.LLen(ctx, c.GetRedisServerApplicationKeyName()); result.Err() != nil {
		panic(result.Err())
	} else {
		batch = int(math.Min(float64(result.Val()), float64(batch)))
	}
	values := client.LPopCount(ctx, c.GetRedisServerApplicationKeyName(), batch)
	return values.Val()
}

// PushApplicationsIntoSeatQueue 将“申请”对应的“申请人”送入“席位”中。返回值为成功送入的总数。
// “席位”采用有序表实现，分数为毫秒。存在不同“申请”分数相同的可能。如果指定的“申请”已存在，则不会更新。“申请”不存在时才会送入有序表中，并计数。
func (c *Activity) PushApplicationsIntoSeatQueue(ctx context.Context, applications []string) int64 {
	if len(applications) == 0 {
		return 0
	}
	count := int64(0)
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	for _, value := range applications {
		if !c.ApplicationExists(ctx, value) {
			continue
		}
		tm := time.Now().UnixMicro()
		result := client.ZAddNX(ctx, c.GetRedisServerSeatKeyName(), redis.Z{
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
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.HExists(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return false
}

// GetApplicant 根据"申请"获取申请人。如果申请人不存在，则返回空字符串。
func (c *Activity) GetApplicant(ctx context.Context, application string) string {
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.HGet(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return ""
}

// GetSeatCount 获取已确定"席位"的数量。
func (c *Activity) GetSeatCount(ctx context.Context) int64 {
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.ZCount(ctx, c.GetRedisServerSeatKeyName(), "-inf", "+inf"); result.Err() == nil {
		return result.Val()
	}
	return 0
}

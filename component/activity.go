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

// ActivityPool represents a pool for activity.
type ActivityPool struct {
	Activities       map[uint64]*Activity // Save all activity records, the key is the activity ID.
	ActivitiesRWLock sync.RWMutex         // A lock to access the activity pool.
}

// InitActivityPool initialize an activity pool.
//
// Usage:
// Activities = InitActivityPool()
//
// Do not declare live pool variables directly unless you know the consequences of doing so.
func InitActivityPool() *ActivityPool {
	a := &ActivityPool{
		Activities: make(map[uint64]*Activity),
	}
	return a
}

// Capacity returns the number of activities.
func (a *ActivityPool) Capacity() int {
	a.ActivitiesRWLock.RLock()
	defer a.ActivitiesRWLock.RUnlock()
	return len(a.Activities)
}

// New create an activity.
// The Activity ID needs to be specified.
// The newly added activity ID cannot be the same as the existing ID, otherwise an ErrActivityExisted error will be returned.
// You can specify the target redis server index. If not specified, it will be number 0.
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

// GetActivity returns the activity pointer for the specified activity ID.
// If the activity with the specified ID does not exist, return a null pointer and ErrActivityNotExist error.
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

// Remove removes the activity with specified ID.
// If the activity with the specified ID does not exist, an ErrActivityNotExist error will be returning.
// If the activity is working but does not stop before being removed, an ErrWorkerIsWorking error will be reported.
// If nil is returning, the removal is successful.
// TODO: Delete the data located in redis after deleting the corresponding activity.
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

// RemoveAll removes all activities and return the number of successful removals.
// If there is an activity in progress, it will be stopped first.
// TODO: Delete the data located in redis after deleting the corresponding activity.
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

// Status returns the status of all activities, such as whether it is working or not,
// and the index the redis server where the data is located.
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

// StopAll stops the worker coroutines of all activities,
// and returns the number of activities that were successfully stopped.
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

// Activity represents an activity.
type Activity struct {
	ID                      uint64
	RedisServerIndex        uint8                   `json:"redis_server_index" default:"0"` //
	Batch                   uint16                  `json:"batch" default:"10000"`          // The number of applications processed in each batch.
	contextCancelFuncRWLock sync.RWMutex            // A lock for manipulating the context cancellation handle.
	contextCancelFunc       context.CancelCauseFunc // context cancellation handle
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

// ErrWorkerStopped indicates that the worker has stopped.
var ErrWorkerStopped = errors.New("the worker stopped")

// Start a worker coroutine for an activity.
//
// Returns nil if started successfully.
// If the redis client is invalid, an ErrRedisClientNil error will be returned.
// If the corresponding activity has already started the worker coroutine, an ErrWorkerIsWorking error will be returned.
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

// A batch is taken from the application queue and sent into the seat for confirmation.
//
// This method relies on the redis function "pop_applications_and_push_into_seats".
// Therefore, before using this method, you need to prepare relevant redis functions.
// Since this method is executed by the Lua script of redis,
// this operation can avoid failure due to changes in related keys,
// and it will not cause out-of-sequence that may be caused by simultaneous execution of multiple worker processes,
// and it can also shorten the interaction time with the client.
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

// Stop a worker coroutine for an activity.
//
// Return nil if stopped successfully.
// A stop must specify a reason. If it is successfully stopped, ErrWorkerStopped is passed in.
// If the worker coroutine for the specified activity has stopped, an ErrWorkerHasBeenStopped err will be returned.
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

// IsWorking determine whether the current coroutine for activity is working.
func (c *Activity) IsWorking() bool {
	// c.contextCancelFuncRWLock.RLock()
	// defer c.contextCancelFuncRWLock.RUnlock()
	return c.contextCancelFunc != nil
}

package component

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setupWorker(t *testing.T) {
	Activities = InitActivityPool()
	if Activities == nil {
		t.Error("Activity Pool should not be nil.")
	}
}

func teardownWorker(t *testing.T) {
	if Activities != nil {
		Activities.RemoveAll()
	}
	Activities = nil
}

func setupWorkerActivity(t *testing.T, activityID uint64) {
	if Activities == nil {
		t.Error("Activity Pool should not be nil.")
	}
	err := Activities.New(activityID, nil)
	if err != nil {
		t.Error("It should not report error when the new activity being inserted.")
	}
}

func teardownWorkerActivity(t *testing.T, activityID uint64) {
	if Activities == nil {
		t.Error("Activity Pool should not be nil.")
	}
	err := Activities.Remove(activityID, true)
	if err != nil {
		t.Error("It should not report error when the activity being removed.")
	}
}

// TestWorker_ActivityExists 测试活动存在时工作协程。
func TestWorker_ActivityExists(t *testing.T) {
	setupWorker(t)
	defer teardownWorker(t)

	// 测试创建协程时不指定处理方法和处理结束后方法。
	t.Run("with default processFunc and doneFunc", func(t *testing.T) {
		activityID := uint64(time.Now().Unix())
		setupWorkerActivity(t, activityID)
		defer teardownWorkerActivity(t, activityID)

		activity, err := Activities.GetActivity(activityID)
		assert.Nil(t, err)                      // The activity has been successfully added and should not throw an error.
		assert.NotNil(t, activity)              // Activity should not be nil.
		assert.IsType(t, &Activity{}, activity) // The activity type should be the same as the default.
		assert.False(t, activity.IsWorking())   // The current activity is not working.

		activity.contextCancelFuncRWLock.Lock()

		// If the context cancel function handle is not nil at this time, it is considered an abnormal state.
		if !assert.Nil(t, activity.contextCancelFunc, "The context cancel function handle should be nil.") {
			activity.contextCancelFuncRWLock.Unlock()
		}

		ctxChild, cancel := context.WithCancelCause(context.Background())
		activity.contextCancelFunc = cancel
		activity.contextCancelFuncRWLock.Unlock()

		go worker(ctxChild, 1, activity.ID, nil, nil)

		assert.True(t, activity.IsWorking(), "The activity should be working.")
		time.Sleep(time.Microsecond * 10) // With an interval of 1 millisecond, it should output about 10 times after a 10 millisecond pause.

		err = activity.Stop(ErrWorkerStopped)
		assert.Nil(t, err, "No error should be reported.")
		assert.False(t, activity.IsWorking(), "The activity should not be working.")
		err = activity.Stop(ErrWorkerStopped)
		assert.IsType(t, ErrWorkerHasBeenStopped, err, "If the stopped task stops again, `ErrWorkerHasBeenStopped` should be reported.")
	})

	//
}

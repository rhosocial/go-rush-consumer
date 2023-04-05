package component

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
	err := Activities.New(activityID)
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

func TestWorker_ActivityExists(t *testing.T) {
	setupWorker(t)
	defer teardownWorker(t)

	t.Run("with default processFunc and doneFunc", func(t *testing.T) {
		activityID := uint64(time.Now().Unix())
		setupWorkerActivity(t, activityID)
		defer teardownWorkerActivity(t, activityID)

		activity, err := Activities.GetActivity(activityID)
		assert.Nil(t, err)
		assert.NotNil(t, activity)
		assert.IsType(t, &Activity{}, activity)
		assert.False(t, activity.IsWorking())

		activity.contextCancelFuncRWLock.Lock()

		if !assert.Nil(t, activity.contextCancelFunc) {
			activity.contextCancelFuncRWLock.Unlock()
		}

		ctxChild, cancel := context.WithCancelCause(context.Background())
		activity.contextCancelFunc = cancel

		go worker(ctxChild, 1, activity.ID, nil, nil)
		activity.contextCancelFuncRWLock.Unlock()

		assert.True(t, activity.IsWorking())
		time.Sleep(time.Microsecond * 10)

		activity.contextCancelFunc(ErrWorkerStopped)
		activity.contextCancelFunc = nil
	})
}

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
		assert.Nil(t, err)                      // 已成功添加活动，则不应当报错。
		assert.NotNil(t, activity)              // 活动应当由内容。
		assert.IsType(t, &Activity{}, activity) // 活动类型应当与默认一致。
		assert.False(t, activity.IsWorking())   // 当前活动处于未工作状态。

		activity.contextCancelFuncRWLock.Lock()

		// 如果此时上下文取消函数句柄不为空，则视为异常状态。
		if !assert.Nil(t, activity.contextCancelFunc, "The context cancel function handle should be nil.") {
			activity.contextCancelFuncRWLock.Unlock()
		}

		ctxChild, cancel := context.WithCancelCause(context.Background())
		activity.contextCancelFunc = cancel
		activity.contextCancelFuncRWLock.Unlock()

		go worker(ctxChild, 1, activity.ID, nil, nil)

		assert.True(t, activity.IsWorking(), "The activity should be working.")
		time.Sleep(time.Microsecond * 10) // interval 为 1 毫秒，暂停 10 毫秒后应当输出 10 次左右。

		err = activity.Stop(ErrWorkerStopped)
		assert.Nil(t, err, "No error should be reported.")
		assert.False(t, activity.IsWorking(), "The activity should not be working.")
		err = activity.Stop(ErrWorkerStopped)
		assert.IsType(t, ErrWorkerHasBeenStopped, err, "If the stopped task stops again, `ErrWorkerHasBeenStopped` should be reported.")
	})
}

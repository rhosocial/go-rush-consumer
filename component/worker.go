package component

import (
	"context"
	"errors"
	"log"
	"time"
)

// doneFunc 输出指定 activityID 工作结束日志。
var doneFunc = func(ctx context.Context, activityID uint64, cause error) {
	if cause == nil || cause == ErrWorkerStopped {
		log.Printf("[ActivityID: %d] worker done.\n", activityID)
	} else {
		log.Printf("[ActivityID: %d] worker exited abnormally, because: %s\n", activityID, cause.Error())
	}
}

var ErrWorkerProcessNil = errors.New("worker process nil")

var deferredWorkerHandlerFunc = func(activity *Activity) {
	if activity == nil {
		log.Println(recover())
	} else if err := recover(); err != nil {
		log.Println(err)
		if err = activity.Stop(err.(error)); err != nil {
			log.Println(err)
		}
	}
}

// worker 处理活动。
// 指定 activityID 的活动必须存在，否则将报错。
// 必须指定处理函数 process。
// 可以不指定处理结束函数 done。如果不指定，则使用默认函数 doneFunc。
func worker(ctx context.Context, interval uint16, activityID uint64, process func(context.Context, uint64), done func(context.Context, uint64, error)) {
	if process == nil {
		panic(ErrWorkerProcessNil)
	}
	if done == nil {
		done = doneFunc
	}

	activity, err := Activities.GetActivity(activityID)
	defer deferredWorkerHandlerFunc(activity)
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		select {
		case <-ctx.Done():
			done(ctx, activityID, context.Cause(ctx))
			return
		default:
			process(ctx, activityID)
		}
	}
}

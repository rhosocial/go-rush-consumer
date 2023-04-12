package component

import (
	"context"
	"log"
	"time"
)

var processFuncDefault = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working (with no action(s))...\n", activityID)
}

// doneFunc 输出指定 activityID 工作结束日志。
var doneFuncDefault = func(ctx context.Context, activityID uint64, cause error) {
	if cause == nil || cause == ErrWorkerStopped {
		log.Printf("[ActivityID: %d] worker done.\n", activityID)
	} else {
		log.Printf("[ActivityID: %d] worker exited abnormally, because: %s\n", activityID, cause.Error())
	}
}

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
// process 为处理方法，可以为 nil。如果为 nil，则采用 processFuncDefault。
// done 为处理结束后方法，可以为 nil。如果为 nil，则采用 doneFuncDefault。
func worker(ctx context.Context, interval uint16, activityID uint64, process func(context.Context, uint64), done func(context.Context, uint64, error)) {
	if process == nil {
		process = processFuncDefault
	}
	if done == nil {
		done = doneFuncDefault
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

package component

import (
	"context"
	"log"
	"time"
)

// doneFunc 输出指定 activityID 工作结束日志。
var doneFunc = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] worker done.\n", activityID)
}

// worker 处理活动。
// 指定 activityID 的活动必须存在，否则将报错。
// 必须指定处理函数 process。
// 可以不指定处理结束函数 done。如果不指定，则使用默认函数 doneFunc。
func worker(ctx context.Context, interval uint16, activityID uint64, process func(context.Context, uint64), done func(context.Context, uint64)) {
	activity, err := Activities.GetActivity(activityID)
	defer func(activity *Activity) {
		if activity == nil {
			log.Println(recover())
		} else if err := activity.Stop(); err != nil && err != ErrWorkerHasBeenStopped {
			log.Println(err)
		}
		if err := recover(); err != nil {
			log.Println(err)
		}
	}(activity)
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		select {
		case <-ctx.Done():
			if done == nil {
				done = doneFunc
			}
			done(ctx, activityID)
			return
		default:
			process(ctx, activityID)
		}
	}
}

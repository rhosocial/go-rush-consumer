package component

import (
	"context"
	"log"
	"time"
)

func worker(ctx context.Context, interval uint16, activityID uint64, process func(context.Context, uint64), done func(context.Context, uint64)) {
	activity, err := Activities.GetActivity(activityID)
	defer func(activity *Activity) {
		if err := activity.Stop(); err != nil && err != ErrWorkerHasBeenStopped {
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
			done(ctx, activityID)
			return
		default:
			process(ctx, activityID)
		}
	}
}

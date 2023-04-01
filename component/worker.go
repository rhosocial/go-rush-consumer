package component

import (
	"context"
	"fmt"
	"time"
)

func worker(ctx context.Context, interval uint16, activityID uint64, process func(context.Context, uint64)) {
	_, err := Activities.GetActivity(activityID)
	if err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Duration(interval) * time.Millisecond)
		select {
		case <-ctx.Done():
			fmt.Println("worker done")
			return
		default:
			process(ctx, activityID)
		}
	}
}

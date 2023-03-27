package component

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func worker(ctx context.Context, interval time.Duration, activityID uint64) {
	activity, err := Activities.GetActivity(activityID)
	if err != nil {
		panic(err)
	}

	defer func(activity *Activity) {
		err := activity.Close()
		if err != nil {
			panic(err)
		}
	}(activity)
	for {
		time.Sleep(interval)
		select {
		case <-ctx.Done():
			fmt.Println("worker done")
			return
		default:
			process(ctx, activityID)
		}
	}
}

func process(ctx context.Context, activityID uint64) {
	fmt.Printf("[%d]: working...\n", time.Now().Unix())
	activity, err := Activities.GetActivity(activityID)
	if err != nil {
		panic(err)
	}
	results := activity.PopApplicationsFromQueue(ctx)
	output := "<empty set>"
	if len(results) > 0 {
		output = strings.Join(results, ", ")
		count := activity.PushApplicationsIntoSeatQueue(ctx, results)
		println(fmt.Sprintf("%d application(s) accepted.", count))
	}
	println("Results:", output)
}

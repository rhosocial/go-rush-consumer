package component

import (
	"context"
	"log"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	commonComponent "github.com/rhosocial/go-rush-common/component"
)

// processFunc 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。但强烈不推荐此做法，除非对性能要求不高，且可以保证处理进程只有一个，否则，因“取”和
// “存”之间并非原子操作而导致乱序。此处仅做流程展示，并非用在实际应用中。
var processFunc = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	// 活动必须存在。
	if err != nil {
		panic(err)
	}
	// 从“申请”队列获取一批“申请”
	results := activity.PopApplicationsFromQueue(ctx)
	output := "<empty set>"
	// 如果有“申请”，则处理。否则按“空”。
	if len(results) > 0 {
		output = strings.Join(results, ", ")
		// 将“申请”对应的“申请人”送入“席位”中。已经存在的申请则忽略，未存在的按顺序依次列在最后。
		count := activity.PushApplicationsIntoSeatQueue(ctx, results)
		log.Printf("[ActivityID: %d] %d application(s) accepted.\n", activityID, count)
	}
	log.Printf("[ActivityID: %d] Results: %s", activityID, output)
}

// processFunc2 工作协程。
// 从申请队列中取出一批，再送入席位
// 以下方式为从“申请”队列中取得一批“申请”，然后再送入“席位”有序表。但依然不推荐此做法，除非可以保证处理进程只有一个，且处理期间“席位”没有发生修改，
// 否则，因“存”时频繁监控到“席位”有序表已发生修改而失败。
var processFunc2 = func(ctx context.Context, activityID uint64) {
	log.Printf("[ActivityID: %d] working...\n", activityID)
	activity, err := Activities.GetActivity(activityID)
	client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err != nil {
		panic(err)
	}
	txf := func(tx *redis.Tx) error {
		// 取得一批的最大值。该值不推荐设太大，否则在高频存取时，可能会因乐观锁频繁失效而无法成功执行。
		batch := int(*(*(*GlobalEnv).Activity).Batch)
		if result := tx.LLen(ctx, activity.GetRedisServerApplicationKeyName()); result.Err() != nil {
			return result.Err()
		} else {
			batch = int(math.Min(float64(result.Val()), float64(batch)))
		}

		// 从“申请”队列获取一批“申请”
		applications := tx.LPopCount(ctx, activity.GetRedisServerApplicationKeyName(), batch).Val()
		output := "<empty set>"

		// 如果有“申请”，则处理。否则按“空”。
		if len(applications) > 0 {
			output = strings.Join(applications, ", ")
			// 将“申请”对应的“申请人”送入“席位”中。已经存在的申请则忽略，未存在的按顺序依次列在最后。
			count := int64(0)
			for _, application := range applications {
				// 如果“申请人”不存在，则忽略。
				if result := tx.HExists(ctx, activity.GetRedisServerApplicantKeyName(), application); result.Err() != nil || result.Val() == false {
					continue
				}
				// 取得“申请人”。
				applicantResult := client.HGet(ctx, activity.GetRedisServerApplicantKeyName(), application)
				if result := client.ZAddNX(ctx, activity.GetRedisServerSeatKeyName(), redis.Z{
					Score:  float64(time.Now().UnixMicro()),
					Member: applicantResult.Val(),
				}); result.Err() == nil {
					count += result.Val()
				} else {
					return result.Err()
				}
			}
			log.Printf("[ActivityID: %d] %d application(s) accepted.\n", activityID, count)
		}
		log.Printf("[ActivityID: %d] Results: %s", activityID, output)
		return nil
	}
	if err := client.Watch(ctx, txf, activity.GetRedisServerApplicantKeyName(), activity.GetRedisServerSeatKeyName(), activity.GetRedisServerApplicationKeyName()); err == redis.TxFailedErr {
		log.Printf("[ActivityID: %d]: %s\n", activityID, err.Error())
	} else if err != nil {
		log.Printf("[ActivityID: %d]: %s\n", activityID, err.Error())
	}
}

// PopApplicationsFromQueue 从申请队列中取出。
func (c *Activity) PopApplicationsFromQueue(ctx context.Context) []string {
	batch := int(*(*(*GlobalEnv).Activity).Batch)
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.LLen(ctx, c.GetRedisServerApplicationKeyName()); result.Err() != nil {
		panic(result.Err())
	} else {
		batch = int(math.Min(float64(result.Val()), float64(batch)))
	}
	values := client.LPopCount(ctx, c.GetRedisServerApplicationKeyName(), batch)
	return values.Val()
}

// PushApplicationsIntoSeatQueue 将“申请”对应的“申请人”送入“席位”中。返回值为成功送入的总数。
// “席位”采用有序表实现，分数为毫秒。存在不同“申请”分数相同的可能。如果指定的“申请”已存在，则不会更新。“申请”不存在时才会送入有序表中，并计数。
func (c *Activity) PushApplicationsIntoSeatQueue(ctx context.Context, applications []string) int64 {
	if len(applications) == 0 {
		return 0
	}
	count := int64(0)
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	for _, value := range applications {
		if !c.ApplicationExists(ctx, value) {
			continue
		}
		tm := time.Now().UnixMicro()
		result := client.ZAddNX(ctx, c.GetRedisServerSeatKeyName(), redis.Z{
			Score:  float64(tm),
			Member: c.GetApplicant(ctx, value),
		})
		if result.Err() == nil {
			count += result.Val()
		} else {
			log.Println(result.Err())
		}
	}
	return count
}

// ApplicationExists 判断"申请"是否存在。
func (c *Activity) ApplicationExists(ctx context.Context, application string) bool {
	if len(application) == 0 {
		return false
	}
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.HExists(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return false
}

// GetApplicant 根据"申请"获取申请人。如果申请人不存在，则返回空字符串。
func (c *Activity) GetApplicant(ctx context.Context, application string) string {
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.HGet(ctx, c.GetRedisServerApplicantKeyName(), application); result.Err() == nil {
		return result.Val()
	}
	return ""
}

// GetSeatCount 获取已确定"席位"的数量。
func (c *Activity) GetSeatCount(ctx context.Context) int64 {
	client := commonComponent.GlobalRedisClientPool.GetClient(&c.RedisServerIndex)
	if result := client.ZCount(ctx, c.GetRedisServerSeatKeyName(), "-inf", "+inf"); result.Err() == nil {
		return result.Val()
	}
	return 0
}

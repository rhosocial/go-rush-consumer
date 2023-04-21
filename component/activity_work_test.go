package component

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/stretchr/testify/assert"
)

func setupActivityWork(t *testing.T) {
	err := LoadEnvDefault()
	if err != nil {
		t.Error(err)
	}
	commonComponent.GlobalRedisClientPool = &commonComponent.RedisClientPool{}
	commonComponent.GlobalRedisClientPool.InitRedisClientPool(&[]commonComponent.EnvRedisServer{
		{
			Host:     "localhost",
			Port:     6379,
			Username: "",
			Password: "",
			DB:       0,
		},
	})
	index := uint8(0)
	content, err := os.ReadFile("go-rush-consumer.lua")
	if err != nil {
		t.Error(err)
	}
	commonComponent.GlobalRedisClientPool.GetClient(&index).FunctionLoadReplace(context.Background(), string(content))
	Activities = InitActivityPool()
}

func teardownActivityWork(t *testing.T) {
	if Activities.Capacity() > 0 {
		Activities.RemoveAll()
	}
	Activities = nil
	commonComponent.GlobalRedisClientPool = nil
}

func setupActivityWorkCase(t *testing.T, activityID uint64) {
	err := Activities.New(activityID, nil)
	if err != nil {
		t.Error(err)
	}
}

func teardownActivityWorkCase(t *testing.T, activityID uint64) {
	activity, err := Activities.GetActivity(activityID)
	if err != nil {
		t.Error(err)
	}
	client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err = client.Close(); err != nil {
		t.Error(err)
	}
	if err = Activities.Remove(activityID, true); err != nil {
		t.Error(err)
	}
}

// randomStringSlice 生成随机数量的字符串切片，也可以生成指定数量。
func randomStringSlice(count *uint16, prefix string) *[]string {
	total := rand.Uint32() % (1 << 16)
	if count != nil && *count > 0 {
		total = uint32(*count)
	}
	var result []string
	for i := 0; uint32(i) < total; i++ {
		result = append(result, fmt.Sprintf("%s%d", prefix, i))
	}
	return &result
}

// randomPairSlice 生成两个字符串切片的随机组合。
func randomPairSlice(slice1 *[]string, slice2 *[]string) *[]string {
	if slice1 == nil || slice2 == nil {
		return nil
	}
	result := make([]string, 2*(len(*slice1)-1))
	for i := 0; i < 2*(len(*slice1)-1); i += 2 {
		result[i] = (*slice1)[int(rand.Uint32()%uint32(len(*slice1)))]
		result[i+1] = (*slice2)[int(rand.Uint32()%uint32(len(*slice2)))]
	}
	return &result
}

func TestWorking_Key(t *testing.T) {
	setupActivityWork(t)
	defer teardownActivityWork(t)

	t.Run("ApplicationKey_PushStringKeyAndPop", func(t *testing.T) {
		activityID := uint64(time.Now().Unix())
		setupActivityWorkCase(t, activityID)
		defer teardownActivityWorkCase(t, activityID)

		activity, err := Activities.GetActivity(activityID)
		if err != nil {
			t.Error(err)
		}

		assert.False(t, activity.IsWorking())

		client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
		assert.NotNil(t, client)

		count := uint16(rand.Uint32() % (1 << 8))
		applications := randomStringSlice(&count, "application_")

		for i, v := range *applications {
			result := client.RPush(context.Background(), activity.GetRedisServerApplicationKeyName(), v)
			if result.Err() != nil {
				t.Errorf("%d-th application insertion: %s", i, result.Err())
				return
			}
		}
		resultPop := client.LPopCount(context.Background(), activity.GetRedisServerApplicationKeyName(), int(count))
		if resultPop.Err() != nil {
			t.Error(resultPop.Err())
		}
		assert.NotNil(t, resultPop.Val())
		assert.Len(t, resultPop.Val(), int(count))
		for i, v := range resultPop.Val() {
			assert.Equal(t, (*applications)[i], v)
		}
	})
}

func TestWorking_ConfirmSeatsAfterApplications(t *testing.T) {
	setupActivityWork(t)
	defer teardownActivityWork(t)

	t.Run("256 applicants with 4096 Applications", func(t *testing.T) {
		activityID := uint64(time.Now().Unix())
		setupActivityWorkCase(t, activityID)
		defer teardownActivityWorkCase(t, activityID)

		activity, err := Activities.GetActivity(activityID)
		if err != nil {
			t.Error(err)
		}

		assert.False(t, activity.IsWorking())

		client := commonComponent.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
		applicationCount := uint16(1 << 12)
		applications := randomStringSlice(&applicationCount, "application_")
		applicantCount := uint16(1 << 8)
		applicants := randomStringSlice(&applicantCount, "applicant_")

		for i, v := range *applications {
			result := client.RPush(context.Background(), activity.GetRedisServerApplicationKeyName(), v)
			if result.Err() != nil {
				t.Errorf("%d-th application insertion: %s", i, result.Err())
				return
			}
		}

		applicantByApplicationPairs := randomPairSlice(applications, applicants)
		client.HSet(context.Background(), activity.GetRedisServerApplicantKeyName(), *applicantByApplicationPairs)

		if err := activity.Start(context.Background()); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(3 * time.Second)
		if err := activity.Stop(ErrWorkerStopped); err != nil {
			t.Error(err)
			return
		}
	})
}

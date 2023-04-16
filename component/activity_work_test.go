package component

import (
	"context"
	"fmt"
	"math/rand"
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

func randomStringSlice(count *uint16) *[]string {
	total := rand.Uint32() % (1 << 16)
	if count != nil && *count > 0 {
		total = uint32(*count)
	}
	var result []string
	for i := 0; uint32(i) < total; i++ {
		result = append(result, fmt.Sprintf("application_%d", i))
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
		applications := randomStringSlice(&count)

		for i, v := range *applications {
			result := client.RPush(context.Background(), activity.GetRedisServerApplicationKeyName(), v)
			if result.Err() != nil {
				t.Errorf("%d-th application insertion: %s", i, result.Err())
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

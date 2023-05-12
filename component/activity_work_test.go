package component

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rhosocial/go-rush-common/component/environment"
	"github.com/rhosocial/go-rush-common/component/redis"
	"github.com/stretchr/testify/assert"
)

func setupActivityWork(t *testing.T) {
	err := LoadEnvDefault()
	if err != nil {
		t.Error(err)
	}
	environment.GlobalRedisClientPool = &redis.ClientPool{}
	environment.GlobalRedisClientPool.InitRedisClientPool(&[]redis.EnvRedisServer{
		{
			Host:     "1.n.rho.im",
			Port:     16479,
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
	environment.GlobalRedisClientPool.GetClient(&index).FunctionLoadReplace(context.Background(), string(content))
	Activities = InitActivityPool()
}

func teardownActivityWork(t *testing.T) {
	if Activities.Capacity() > 0 {
		Activities.RemoveAll()
	}
	Activities = nil
	environment.GlobalRedisClientPool = nil
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
	client := environment.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
	if err = client.Close(); err != nil {
		t.Error(err)
	}
	if err = Activities.Remove(activityID, true); err != nil {
		t.Error(err)
	}
}

// randomStringSlice generates no more than 65535 string slices with specified prefix,
// or a specified number of string slices.
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

// randomPairSlice randomly matches two string slices.
//
// The returned format is key1, value1, key2, value2, ...
// The length is twice the length of `slice1` minus 1, that is, at least one member in `slice1` is not selected.
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

// TestWorking_Key checks the pushing and popping of applications.
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

		assert.False(t, activity.IsWorking()) // The newly added activity is not in working.

		client := environment.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)
		assert.NotNil(t, client, "The redis client must be valid.")

		// Randomly generate 256 applications.
		count := uint16(rand.Uint32() % (1 << 8))
		applications := randomStringSlice(&count, "application_")

		// Send them to redis in turn.
		for i, v := range *applications {
			result := client.RPush(context.Background(), activity.GetRedisServerApplicationKeyName(), v)
			if result.Err() != nil {
				t.Errorf("%d-th application insertion: %s", i, result.Err())
				return
			}
		}

		// Check that the contents taken out are in the same order as they were brought in.
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

// TestWorking_ConfirmSeatsAfterApplications
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

		client := environment.GlobalRedisClientPool.GetClient(&activity.RedisServerIndex)

		// Randomly generate 4096 applications.
		applicationCount := uint16(1 << 12)
		applications := randomStringSlice(&applicationCount, "application_")
		// Randomly generate 256 applicants.
		applicantCount := uint16(1 << 8)
		applicants := randomStringSlice(&applicantCount, "applicant_")

		// Send applications generated to redis in turn.
		for i, v := range *applications {
			result := client.RPush(context.Background(), activity.GetRedisServerApplicationKeyName(), v)
			if result.Err() != nil {
				t.Errorf("%d-th application insertion: %s", i, result.Err())
				return
			}
		}

		// Applicants and applications are randomly matched,
		applicantByApplicationPairs := randomPairSlice(applications, applicants)
		// and send them to redis.
		client.HSet(context.Background(), activity.GetRedisServerApplicantKeyName(), *applicantByApplicationPairs)

		// Start a worker,
		if err := activity.Start(context.Background()); err != nil {
			t.Error(err)
			return
		}
		// and wait 3 seconds. This period is long enough to process all applications.
		time.Sleep(3 * time.Second)
		if err := activity.Stop(ErrWorkerStopped); err != nil {
			t.Error(err)
			return
		}
	})
}

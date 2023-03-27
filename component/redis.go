package component

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math"
	"sync"
	"time"
)

var Activities *ActivityPool

type ActivityPool struct {
	Activities       map[uint64]*Activity
	ActivitiesRWLock sync.RWMutex
}

func InitActivityPool() *ActivityPool {
	a := &ActivityPool{
		Activities: make(map[uint64]*Activity),
	}
	return a
}

func (a *ActivityPool) Capacity() int {
	return len(a.Activities)
}

func (a *ActivityPool) New(id uint64) error {
	if _, existed := a.Activities[id]; existed {
		return errors.New("activity id existed")
	}
	a.Activities[id] = &Activity{
		ID:     id,
		Client: nil,
	}
	return nil
}

func (a *ActivityPool) ActivityExists(id uint64) bool {
	_, existed := a.Activities[id]
	return existed
}

func (a *ActivityPool) GetActivity(id uint64) (*Activity, error) {
	activity, existed := a.Activities[id]
	if !existed {
		return nil, errors.New("activity id not exist")
	}
	return activity, nil
}

func (a *ActivityPool) RemoveActivity(id uint64) error {
	if !a.ActivityExists(id) {
		return errors.New("activity not exist")
	}
	delete(a.Activities, id)
	return nil
}

type Activity struct {
	ID                      uint64
	Client                  *redis.Client
	ContextCancelFuncRWLock sync.RWMutex
	ContextCancelFunc       context.CancelFunc
}

type RedisConnection struct {
	Addr     string
	Username string
	Password string
	DB       int
}

func NewRedisConnection(conn *RedisConnection) (*redis.Client, error) {
	if conn == nil {
		return nil, errors.New("invalid redis connection")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     conn.Addr,
		Username: conn.Username,
		Password: conn.Password,
		DB:       conn.DB,
	})
	return rdb, nil
}

func (c *Activity) GetRedisServerApplicationKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Application, c.ID)
}

func (c *Activity) GetRedisServerApplicantKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Applicant, c.ID)
}

func (c *Activity) GetRedisServerSeatKeyName() string {
	return fmt.Sprintf("%s%d", GlobalEnv.Activity.RedisServer.KeyPrefix.Seat, c.ID)
}

func (c *Activity) Connect() error {
	redisIdx := uint8(0)
	client, err := NewRedisConnection(GlobalEnv.GetRedisConnection(&redisIdx))
	if err != nil {
		return err
	}
	c.Client = client
	return nil
}

func (c *Activity) Close() error {
	err := c.Client.Close()
	if err != nil {
		return err
	}
	c.Client = nil
	return nil
}

func (c *Activity) Start(ctx context.Context) error {
	if c.Client == nil {
		err := c.Connect()
		if err != nil {
			return err
		}
	}
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	if c.ContextCancelFunc != nil {
		return errors.New("there is already a worker started")
	}
	ctxChild, cancel := context.WithCancel(ctx)
	c.ContextCancelFunc = cancel
	go worker(ctxChild, time.Second, c.ID)
	return nil
}

func (c *Activity) Stop() error {
	c.ContextCancelFuncRWLock.Lock()
	defer c.ContextCancelFuncRWLock.Unlock()
	cancelFunc := c.ContextCancelFunc
	if cancelFunc == nil {
		return errors.New("the worker has already been stopped")
	}
	cancelFunc()
	c.ContextCancelFunc = nil
	return nil
}

func (c *Activity) PopApplicationsFromQueue(ctx context.Context) []string {
	result := c.Client.LLen(ctx, c.GetRedisServerApplicationKeyName())
	if result.Err() != nil {
		panic(result.Err())
	}
	if result.Val() == 0 {
		return []string{}
	}
	batch := int(math.Min(float64(result.Val()), float64(GlobalEnv.Activity.Batch)))
	values := c.Client.LPopCount(ctx, c.GetRedisServerApplicationKeyName(), batch)
	return values.Val()
}

func (c *Activity) PushApplicationsIntoSeatQueue(ctx context.Context, applications []string) int64 {
	if len(applications) == 0 {
		return 0
	}
	count := int64(0)
	for _, value := range applications {
		if !c.ApplicationExists(ctx, value) {
			continue
		}
		tm := time.Now().UnixMicro()
		result := c.Client.ZAddNX(ctx, c.GetRedisServerSeatKeyName(), redis.Z{
			Score:  float64(tm),
			Member: c.GetApplicant(ctx, value),
		})
		if result.Err() == nil {
			count += result.Val()
		} else {
			println(result.Err())
		}
	}
	return count
}

func (c *Activity) ApplicationExists(ctx context.Context, application string) bool {
	if len(application) == 0 {
		return false
	}
	result := c.Client.HExists(ctx, c.GetRedisServerApplicantKeyName(), application)
	if result.Err() == nil {
		return result.Val()
	}
	return false
}

func (c *Activity) GetApplicant(ctx context.Context, application string) string {
	result := c.Client.HGet(ctx, c.GetRedisServerApplicantKeyName(), application)
	if result.Err() == nil {
		return result.Val()
	}
	return ""
}

func (c *Activity) GetSeatCount(ctx context.Context) int64 {
	result := c.Client.ZCount(ctx, c.GetRedisServerSeatKeyName(), "-inf", "+inf")
	if result.Err() == nil {
		return result.Val()
	}
	return 0
}

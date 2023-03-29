package component

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
)

var GlobalRedisClient *redis.Client
var GlobalRedisClientRWLock sync.RWMutex

func ConnectRedisServer(options *redis.Options) error {
	GlobalRedisClientRWLock.Lock()
	defer GlobalRedisClientRWLock.Unlock()
	if GlobalRedisClient != nil {
		return errors.New("redis client existed")
	}
	GlobalRedisClient = redis.NewClient(options)
	return nil
}

type RedisServerStatus struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
}

func GetRedisServerStatus(ctx context.Context) map[uint8]RedisServerStatus {
	result := make(map[uint8]RedisServerStatus)
	for i, c := range GlobalEnv.RedisServers {
		status := RedisServerStatus{
			Valid: false,
		}
		client := redis.NewClient(c.GetRedisOptions())
		poolStats := client.PoolStats()
		if _, err := client.Ping(ctx).Result(); err != nil {
			status.Valid = false
			status.Message = err.Error()
		} else {
			status.Valid = true
			status.Message = fmt.Sprintf("命中:%d, 未命中:%d, 超时:%d, 总连接:%d, 空闲连接:%d, 失效连接:%d.", poolStats.Hits, poolStats.Misses, poolStats.Timeouts, poolStats.TotalConns, poolStats.IdleConns, poolStats.StaleConns)
		}
		result[uint8(i)] = status
	}
	return result
}

package component

import (
	"context"
	"fmt"
)

type RedisServerStatus struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
}

func GetRedisServerStatus(ctx context.Context) map[uint8]RedisServerStatus {
	result := make(map[uint8]RedisServerStatus)
	for i, c := range *GlobalEnv.GetRedisClients() {
		status := RedisServerStatus{
			Valid: false,
		}
		poolStats := c.PoolStats()
		if _, err := c.Ping(ctx).Result(); err != nil {
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

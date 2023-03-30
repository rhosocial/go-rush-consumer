package component

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"sync/atomic"
	"time"
)

type EnvNet struct {
	ListenPort uint16 `yaml:"ListenPort" default:"8080"`
}

type EnvRedisServerDialer struct {
	KeepAlive uint8 `yaml:"KeepAlive,omitempty" default:"5"`
	Timeout   uint8 `yaml:"Timeout,omitempty" default:"1"`
}

func (c *EnvRedisServer) GetDialerDefault() *EnvRedisServerDialer {
	d := EnvRedisServerDialer{}
	return &d
}

type EnvRedisServer struct {
	Host     string                `yaml:"Host,omitempty" default:"localhost"`
	Port     uint16                `yaml:"Port,omitempty" default:"6379"`
	Username string                `yaml:"Username,omitempty" default:""`
	Password string                `yaml:"Password,omitempty" default:""`
	DB       int                   `yaml:"DB,omitempty" default:"0"`
	Weight   uint8                 `yaml:"Weight,omitempty" default:"1"`
	Dialer   *EnvRedisServerDialer `yaml:"Dialer,omitempty"`
}

type EnvActivityRedisServerKeyPrefix struct {
	Application string `yaml:"Application,omitempty" default:"activity_application_"`
	Applicant   string `yaml:"Applicant,omitempty" default:"activity_applicant_"`
	Seat        string `yaml:"Seat,omitempty" default:"activity_seat_"`
}

type EnvActivityRedisServer struct {
	KeyPrefix EnvActivityRedisServerKeyPrefix `yaml:"KeyPrefix"`
}

type EnvActivity struct {
	RedisServer EnvActivityRedisServer `yaml:"RedisServer"`
	Batch       uint8                  `yaml:"Batch,omitempty" default:"100"`
}

type Env struct {
	Net          EnvNet           `yaml:"Net"`
	RedisServers []EnvRedisServer `yaml:"RedisServers"`
	Activity     EnvActivity      `yaml:"Activity"`
	redisClients *[]*redis.Client
}

func (e *Env) InitRedisClients() {
	redisClients := make([]*redis.Client, len(e.RedisServers))
	for i, v := range e.RedisServers {
		redisClients[i] = redis.NewClient(v.GetRedisOptions())
	}
	e.redisClients = &redisClients
}

func (e *Env) GetRedisClients() *[]*redis.Client {
	if e.redisClients == nil {
		e.InitRedisClients()
	}
	return e.redisClients
}

var redisClientTurnIndex atomic.Int64
var redisClientTurnMap []uint8

// GetCurrentRedisClient 用于获取当前轮次的 redis 客户端。
// 如果当前没有有效客户端，则返回 nil。
// 如果当前只有一个客户端，则只返回该客户端，且不会改变轮候次序。
func (e *Env) GetCurrentRedisClient() *redis.Client {
	if len(e.RedisServers) == 0 {
		return nil
	}
	if len(e.RedisServers) == 1 {
		return (*e.GetRedisClients())[0]
	}
	if len(redisClientTurnMap) == 0 {
		redisClientTurnMap = GetRedisClientTurnMap()
	}
	turn := redisClientTurnMap[redisClientTurnIndex.Add(1)%int64(len(redisClientTurnMap))]
	return (*e.GetRedisClients())[turn]
}

var GlobalEnv Env

func LoadEnvFromYaml(filepath string) error {
	var env Env
	file, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(file, &env)
	if err != nil {
		return err
	}
	env.InitRedisClients()
	GlobalEnv = env
	return nil
}

func LoadEnvFromDefaultYaml() error {
	return LoadEnvFromYaml("default.yaml")
}

func (e *Env) GetRedisConnectionCount() int {
	return len(GlobalEnv.RedisServers)
}

func (e *Env) GetRedisServerTurns() []uint8 {
	turns := make([]uint8, len(GlobalEnv.RedisServers))
	for i, v := range GlobalEnv.RedisServers {
		turns[i] = v.Weight
	}
	return turns
}

func (e *EnvRedisServer) GetRedisOptions() *redis.Options {
	options := redis.Options{
		Addr:     fmt.Sprintf("%s:%d", e.Host, e.Port),
		Username: e.Username,
		Password: e.Password,
		DB:       e.DB,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			config := e.Dialer
			if config == nil {
				config = e.GetDialerDefault()
			}
			netDialer := &net.Dialer{
				Timeout:   time.Duration(config.Timeout) * time.Second,
				KeepAlive: time.Duration(config.KeepAlive) * time.Minute,
			}
			return netDialer.Dial(network, address)
		},
	}
	return &options
}

func GetRedisClientTurnMap() []uint8 {
	turns := GlobalEnv.GetRedisServerTurns()
	turnMap := make([]uint8, 0)
	for i, v := range turns {
		for j := 0; j < int(v); j++ {
			turnMap = append(turnMap, uint8(i))
		}
	}
	redisClientTurnMap = turnMap
	return turnMap
}

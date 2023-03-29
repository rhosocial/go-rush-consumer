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

type EnvRedisServer struct {
	Host     string `yaml:"Host,omitempty" default:"localhost"`
	Port     uint16 `yaml:"Port,omitempty" default:"6379"`
	Username string `yaml:"Username,omitempty" default:""`
	Password string `yaml:"Password,omitempty" default:""`
	DB       int    `yaml:"DB,omitempty" default:"0"`
	Weight   uint8  `yaml:"Weight,omitempty" default:"1"`
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
			netDialer := &net.Dialer{
				Timeout:   1 * time.Second,
				KeepAlive: 5 * time.Minute,
			}
			return netDialer.Dial(network, address)
		},
	}
	return &options
}

var redisClientTurnIndex atomic.Int64
var redisClientTurnMap []uint8

func GetRedisClientTurnMap() []uint8 {
	if len(redisClientTurnMap) > 0 {
		return redisClientTurnMap
	}
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

func GetRedisOptions() *redis.Options {
	return GlobalEnv.RedisServers[GetRedisClientTurnMap()[redisClientTurnIndex.Add(1)%int64(len(GetRedisClientTurnMap()))]].GetRedisOptions()
}

func GetRedisClient() *redis.Client {
	return redis.NewClient(GetRedisOptions())
}

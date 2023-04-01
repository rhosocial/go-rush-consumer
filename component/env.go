package component

import (
	"github.com/redis/go-redis/v9"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"gopkg.in/yaml.v3"
	"os"
)

type EnvNet struct {
	ListenPort uint16 `yaml:"ListenPort" default:"8080"`
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
	Net          EnvNet                           `yaml:"Net"`
	RedisServers []commonComponent.EnvRedisServer `yaml:"RedisServers"`
	Activity     EnvActivity                      `yaml:"Activity"`
	redisClients *[]*redis.Client
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
	commonComponent.GlobalRedisClientPool = &commonComponent.RedisClientPool{}
	commonComponent.GlobalRedisClientPool.InitRedisClientPool(&env.RedisServers)
	currentClient = commonComponent.GlobalRedisClientPool.GetCurrentClient
	GlobalEnv = env
	return nil
}

func LoadEnvFromDefaultYaml() error {
	return LoadEnvFromYaml("default.yaml")
}

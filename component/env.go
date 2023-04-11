package component

import (
	"log"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"gopkg.in/yaml.v3"
)

type EnvNet struct {
	ListenPort *uint16 `yaml:"ListenPort,omitempty" default:"8080"`
}

func (e *EnvNet) Validate() error {
	listen := uint16(80)
	if e.ListenPort == nil {
		e.ListenPort = &listen
	}
	return nil
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
	Net          *EnvNet                          `yaml:"Net,omitempty"`
	RedisServers []commonComponent.EnvRedisServer `yaml:"RedisServers"`
	Activity     EnvActivity                      `yaml:"Activity"`
	redisClients *[]*redis.Client
}

func (e *Env) GetEnvDefault() *EnvNet {
	listen := uint16(80)
	env := EnvNet{
		ListenPort: &listen,
	}
	return &env
}

func (e *Env) Validate() error {
	for _, v := range e.RedisServers {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	if e.Net != nil {
		if err := e.Net.Validate(); err != nil {
			return err
		}
	} else {
		e.Net = e.GetEnvDefault()
	}
	return nil
}

var GlobalEnv *Env

func LoadEnvFromYaml(filepath string) error {
	var env Env
	file, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(file, &env); err != nil {
		return nil
	}
	if err := env.Validate(); err != nil {
		return err
	}

	commonComponent.GlobalRedisClientPool = &commonComponent.RedisClientPool{}
	commonComponent.GlobalRedisClientPool.InitRedisClientPool(&env.RedisServers)
	currentClient = commonComponent.GlobalRedisClientPool.GetCurrentClient
	GlobalEnv = &env
	return nil
}

func LoadEnvFromDefaultYaml() error {
	return LoadEnvFromYaml("default.yaml")
}

func LoadEnvFromSystemEnvVar() error {
	var env Env
	env.Validate()
	if GlobalEnv == nil {
		GlobalEnv = &env
	}
	if value, exist := os.LookupEnv("Net.ListenPort"); exist {
		log.Println("Net.ListenPort: ", value)
		ListenPort, _ := strconv.ParseUint(value, 10, 16)
		*(*env.Net).ListenPort = uint16(ListenPort)
	}
	return nil
}

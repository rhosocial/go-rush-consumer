package component

import (
	"log"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/rhosocial/go-rush-common/component/environment"
	componentRedis "github.com/rhosocial/go-rush-common/component/redis"
	"gopkg.in/yaml.v3"
)

type EnvNet struct {
	ListenPort *uint16 `yaml:"ListenPort,omitempty" default:"8080"`
}

func (e *EnvNet) GetListenPortDefault() *uint16 {
	port := uint16(8080)
	return &port
}

func (e *EnvNet) Validate() error {
	if e.ListenPort == nil {
		e.ListenPort = e.GetListenPortDefault()
	}
	return nil
}

type EnvActivityRedisServerKeyPrefix struct {
	Application string `yaml:"Application,omitempty" default:"activity_application_"`
	Applicant   string `yaml:"Applicant,omitempty" default:"activity_applicant_"`
	Seat        string `yaml:"Seat,omitempty" default:"activity_seat_"`
}

type EnvActivityRedisServer struct {
	KeyPrefix *EnvActivityRedisServerKeyPrefix `yaml:"KeyPrefix"`
}

func (e *EnvActivityRedisServer) GetKeyPrefixDefault() *EnvActivityRedisServerKeyPrefix {
	key := EnvActivityRedisServerKeyPrefix{
		Application: "activity_application_",
		Applicant:   "activity_applicant_",
		Seat:        "activity_seat_",
	}
	return &key
}

type EnvActivity struct {
	RedisServer *EnvActivityRedisServer `yaml:"RedisServer"`
	Batch       *uint16                 `yaml:"Batch,omitempty" default:"1000"`
}

func (e *EnvActivity) GetRedisServerDefault() *EnvActivityRedisServer {
	rs := EnvActivityRedisServer{}
	rs.KeyPrefix = rs.GetKeyPrefixDefault()
	return &rs
}

func (e *EnvActivity) GetBatchDefault() *uint16 {
	batch := uint16(1000)
	return &batch
}

func (e *EnvActivity) Validate() error {
	if e.RedisServer == nil {
		e.RedisServer = e.GetRedisServerDefault()
	}
	if e.Batch == nil {
		e.Batch = e.GetBatchDefault()
	}
	return nil
}

type Env struct {
	Net          *EnvNet                          `yaml:"Net,omitempty"`
	RedisServers *[]componentRedis.EnvRedisServer `yaml:"RedisServers,omitempty"`
	Activity     *EnvActivity                     `yaml:"Activity,omitempty"`
	redisClients *[]*redis.Client
}

// GetNetDefault 取得 EnvNet 的默认值。
// EnvNet.ListenPort 默认值为 8080。
func (e *Env) GetNetDefault() *EnvNet {
	net := EnvNet{}
	net.ListenPort = net.GetListenPortDefault()
	return &net
}

// GetRedisServersDefault 取得 EnvRedisServers 的默认值。
// 默认值为只想空数组的指针，表示默认没有 redis 服务器。
func (e *Env) GetRedisServersDefault() *[]componentRedis.EnvRedisServer {
	servers := make([]componentRedis.EnvRedisServer, 0)
	return &servers
}

// GetActivityDefault 取得 EnvActivity 的默认值。
// EnvActivity.RedisServer 为默认参数，详见 EnvActivity.GetRedisServerDefault()。
// EnvActivity.Batch 为默认值，详见 EnvActivity.GetBatchDefault()。
func (e *Env) GetActivityDefault() *EnvActivity {
	env := EnvActivity{}
	env.RedisServer = env.GetRedisServerDefault()
	env.Batch = env.GetBatchDefault()
	return &env
}

// Validate 验证并加载默认值。
// Env 的默认值包括：
// EnvNet
// RedisServers
// Activity
func (e *Env) Validate() error {
	if e.RedisServers == nil {
		e.RedisServers = e.GetRedisServersDefault()
	}
	for _, v := range *e.RedisServers {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	if e.Net == nil {
		e.Net = e.GetNetDefault()
	} else if err := e.Net.Validate(); err != nil {
		return err
	}
	if e.Activity == nil {
		e.Activity = e.GetActivityDefault()
	} else if err := e.Activity.Validate(); err != nil {
		return err
	}
	return nil
}

var GlobalEnv *Env

// LoadEnvDefault 加载配置参数默认值。
func LoadEnvDefault() error {
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	return nil
}

func LoadEnvFromYaml(filepath string) error {
	file, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	if err := yaml.Unmarshal(file, GlobalEnv); err != nil {
		return err
	}
	if err := GlobalEnv.Validate(); err != nil {
		return err
	}

	environment.GlobalRedisClientPool.InitRedisClientPool(GlobalEnv.RedisServers)
	/**
	currentClient = commonComponent.GlobalRedisClientPool.GetCurrentClient
	defer func() {
		if err := recover(); err == commonComponent.ErrRedisClientNil || err == commonComponent.ErrRedisClientsNotAvailable {
			log.Println(err)
		}
	}()
	if currentClient() != nil {
		contents, err := os.ReadFile("component/go-rush-consumer.lua")
		if err != nil {
			println(err)
		}
		currentClient().FunctionLoadReplace(context.Background(), string(contents))
	}*/
	return nil
}

func LoadEnvFromSystemEnvVar() error {
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	if value, exist := os.LookupEnv("Consumer_Net_ListenPort"); exist {
		log.Println("Consumer_Net_ListenPort: ", value)
		port, _ := strconv.ParseUint(value, 10, 16)
		*(*GlobalEnv.Net).ListenPort = uint16(port)
	}
	if value, exist := os.LookupEnv("Consumer_Activity_Batch"); exist {
		log.Println("Consumer_Activity_Batch: ", value)
		batch, _ := strconv.ParseUint(value, 10, 8)
		*(*GlobalEnv.Activity).Batch = uint16(batch)
	}
	return nil
}

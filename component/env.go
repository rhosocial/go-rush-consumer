package component

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
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

func (e *Env) GetRedisConnection(idx *uint8) *RedisConnection {
	i := uint8(0)
	if idx != nil && *idx < 16 && *idx < uint8(len(GlobalEnv.RedisServers)) {
		i = *idx
	}
	conn := RedisConnection{
		Addr:     fmt.Sprintf("%s:%d", GlobalEnv.RedisServers[i].Host, GlobalEnv.RedisServers[i].Port),
		Username: GlobalEnv.RedisServers[i].Username,
		Password: GlobalEnv.RedisServers[i].Password,
		DB:       GlobalEnv.RedisServers[i].DB,
	}
	return &conn
}

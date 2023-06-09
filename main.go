package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-common/component/auth"
	"github.com/rhosocial/go-rush-common/component/environment"
	componentError "github.com/rhosocial/go-rush-common/component/error"
	"github.com/rhosocial/go-rush-common/component/logger"
	"github.com/rhosocial/go-rush-common/component/redis"
	"github.com/rhosocial/go-rush-consumer/component"
	controllerActivity "github.com/rhosocial/go-rush-consumer/controllers/activity"
	"github.com/rhosocial/go-rush-consumer/controllers/server"
)

var r *gin.Engine

func main() {
	log.Println("Hello, World!")

	environment.GlobalRedisClientPool = &redis.ClientPool{}

	// 最初初始化所有配置参数为默认值。
	err := component.LoadEnvDefault()
	if err != nil {
		println(err.Error())
		return
	}

	// 先从文件中加载配置信息。
	if err := component.LoadEnvFromYaml("default.yaml"); err != nil {
		println(err.Error())
	}
	// 再从环境变量中加载配置信息。
	if err := component.LoadEnvFromSystemEnvVar(); err != nil {
		println(err.Error())
	}
	// 都加载错误则使用默认值。
	component.Activities = component.InitActivityPool()
	if err := initExamples(); err != nil {
		println(err.Error())
		return
	}
	r = gin.New()
	if !configEngine(r) {
		return
	}
	SetupCloseHandler()
	if err := r.Run(fmt.Sprintf(":%d", *(*(*component.GlobalEnv).Net).ListenPort)); err != nil {
		log.Println(err.Error())
		return
	}
}

func initExamples() error {
	activityID := uint64(time.Now().Unix())
	if err := component.Activities.New(activityID, nil); err != nil {
		return err
	}
	if _, err := component.Activities.GetActivity(activityID); err != nil {
		return err
	}
	return nil
}

func configEngine(r *gin.Engine) bool {
	r.Use(
		logger.AppendRequestID(),
		gin.LoggerWithFormatter(logger.LogFormatter),
		auth.AuthRequired(),
		gin.Recovery(),
		componentError.ErrorHandler(),
	)
	var cs controllerServer.ControllerServer
	cs.RegisterActions(r)
	var ca controllerActivity.ControllerActivity
	ca.RegisterActions(r)
	return true
}

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our cleaning-up procedure and exiting the program.
func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		<-c
		log.Println("\r- Ctrl+C pressed in Terminal")
		count := component.Activities.StopAll()
		log.Printf("%d worker(s) stopped.\n", count)
		os.Exit(0)
	}()
}

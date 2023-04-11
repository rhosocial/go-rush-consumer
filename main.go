package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-consumer/component"
	controllerActivity "github.com/rhosocial/go-rush-consumer/controllers/activity"
	"github.com/rhosocial/go-rush-consumer/controllers/server"
)

var r *gin.Engine

func main() {
	log.Println("Hello, World!")

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
	if err := r.Run(fmt.Sprintf(":%d", *(*(*component.GlobalEnv).Net).ListenPort)); err != nil {
		log.Println(err.Error())
		return
	}
}

func initExamples() error {
	activityID := uint64(time.Now().Unix())
	if err := component.Activities.New(activityID); err != nil {
		return err
	}
	if _, err := component.Activities.GetActivity(activityID); err != nil {
		return err
	}
	return nil
}

func configEngine(r *gin.Engine) bool {
	r.Use(
		commonComponent.AppendRequestID(),
		gin.LoggerWithFormatter(commonComponent.LogFormatter),
		commonComponent.AuthRequired(),
		gin.Recovery(),
		commonComponent.ErrorHandler(),
	)
	r.GET("/status", controllerServer.ActionStatus)
	var ca controllerActivity.ControllerActivity
	ca.RegisterActions(r)
	return true
}

package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"go-rush-consumer/component"
	controllerActivity "go-rush-consumer/controllers/activity"
	"go-rush-consumer/controllers/server"
	"time"
)

var r *gin.Engine

func main() {
	println("Hello, World!")
	if err := component.LoadEnvFromDefaultYaml(); err != nil {
		println(err.Error())
		return
	}
	component.Activities = component.InitActivityPool()
	activityID := uint64(time.Now().Unix())
	err := component.Activities.New(activityID)
	if err != nil {
		println(err.Error())
		return
	}
	_, err = component.Activities.GetActivity(activityID)
	if err != nil {
		println(err.Error())
		return
	}
	r = gin.New()
	if !configEngine(r) {
		return
	}
	if err := r.Run(fmt.Sprintf(":%d", component.GlobalEnv.Net.ListenPort)); err != nil {
		println(err.Error())
		return
	}
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

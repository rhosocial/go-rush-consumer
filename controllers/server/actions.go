package controllerServer

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-common/component/controller"
	"github.com/rhosocial/go-rush-common/component/environment"
	"github.com/rhosocial/go-rush-common/component/redis"
	"github.com/rhosocial/go-rush-consumer/component"
)

type ActionStatusResponseData struct {
	RedisServers map[uint8]RedisServerStatus         `json:"redis_servers"`
	Activities   map[uint64]component.ActivityStatus `json:"activities"`
}

type RedisServerStatus struct {
	redis.ServerStatus
	LuaModuleVersion string `json:"lua_module_version"`
}

func (a *ControllerServer) ActionStatus(c *gin.Context) {
	if component.Activities == nil {
		c.JSON(http.StatusOK, a.NewResponseGeneric(c, 0, "No activities", nil, nil))
		return
	}
	commonStatus := environment.GlobalRedisClientPool.GetRedisServersStatus(context.Background())
	status := make(map[uint8]RedisServerStatus)
	for i, v := range commonStatus {
		version := ""
		if val, err := environment.GlobalRedisClientPool.GetClient(nil).FCall(context.Background(), "go_rush_consumer_version", nil, nil).Int64Slice(); err == nil {
			version = fmt.Sprintf("%d.%d.%d", val[0], val[1], val[2])
		} else {
			version = err.Error()
		}
		status[i] = RedisServerStatus{
			ServerStatus:     v,
			LuaModuleVersion: version,
		}
	}
	data := ActionStatusResponseData{
		RedisServers: status,
		Activities:   component.Activities.Status(),
	}
	c.JSON(http.StatusOK, a.NewResponseGeneric(c, 0, "success", data, nil))
}

func (a *ControllerServer) ActionRedisServerFunctionLoadReplace(c *gin.Context) {
	code, _ := c.FormFile("code")
	content, _ := code.Open()
	buf := make([]byte, code.Size)
	content.Read(buf)
	c.JSON(http.StatusOK, a.NewResponseGeneric(c, 0, fmt.Sprintf("%d byte(s) uploaded", code.Size), nil, nil))
}

type ControllerServer struct {
	controller.GenericController
}

func (a *ControllerServer) RegisterActions(r *gin.Engine) {
	controller := r.Group("/server")
	{
		controller.GET("", a.ActionStatus)
		controllerRedis := controller.Group("/redis")
		{
			controllerRedisFunction := controllerRedis.Group("/function")
			{
				controllerRedisFunction.POST("/load_replace", a.ActionRedisServerFunctionLoadReplace)
			}
		}
	}
}

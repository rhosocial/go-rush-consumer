package controllerServer

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-consumer/component"
	"net/http"
)

type ActionStatusResponseData struct {
	RedisServers map[uint8]RedisServerStatus         `json:"redis_servers"`
	Activities   map[uint64]component.ActivityStatus `json:"activities"`
}

type RedisServerStatus struct {
	commonComponent.RedisServerStatus
	LuaModuleVersion string `json:"lua_module_version"`
}

func (a *ControllerServer) ActionStatus(c *gin.Context) {
	if component.Activities == nil {
		c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "No activities", nil, nil))
		return
	}
	commonStatus := commonComponent.GlobalRedisClientPool.GetRedisServersStatus(context.Background())
	status := make(map[uint8]RedisServerStatus)
	for i, v := range commonStatus {
		version := ""
		if val, err := commonComponent.GlobalRedisClientPool.GetClient(nil).FCall(context.Background(), "go_rush_consumer_version", nil, nil).Int64Slice(); err == nil {
			version = fmt.Sprintf("%d.%d.%d", val[0], val[1], val[2])
		} else {
			version = err.Error()
		}
		status[i] = RedisServerStatus{
			RedisServerStatus: v,
			LuaModuleVersion:  version,
		}
	}
	data := ActionStatusResponseData{
		RedisServers: status,
		Activities:   component.Activities.Status(),
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "Activities existed", data, nil))
}

type ControllerServer struct {
	commonComponent.GenericController
}

func (a *ControllerServer) RegisterActions(r *gin.Engine) {
	controller := r.Group("/server")
	{
		controller.GET("", a.ActionStatus)
	}
}

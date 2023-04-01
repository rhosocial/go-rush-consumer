package controllerServer

import (
	"context"
	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"go-rush-consumer/component"
	"net/http"
)

type ActionStatusResponseData struct {
	RedisServers map[uint8]commonComponent.RedisServerStatus `json:"redis_servers"`
	Activities   map[uint64]bool                             `json:"activities"`
}

func ActionStatus(c *gin.Context) {
	if component.Activities == nil {
		c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "No activities", nil, nil))
		return
	}
	data := ActionStatusResponseData{
		RedisServers: commonComponent.GlobalRedisClientPool.GetRedisServerStatus(context.Background()),
		Activities:   component.Activities.Status(),
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "Activities existed", data, nil))
}

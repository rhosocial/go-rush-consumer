package controllerServer

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-consumer/component"
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
		RedisServers: commonComponent.GlobalRedisClientPool.GetRedisServersStatus(context.Background()),
		Activities:   component.Activities.Status(),
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "Activities existed", data, nil))
}

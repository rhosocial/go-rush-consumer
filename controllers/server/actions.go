package controllerServer

import (
	"github.com/gin-gonic/gin"
	"go-rush-consumer/component"
	"net/http"
)

type ActionStatusResponseData struct {
	IsConnected bool `json:"is_connected"`
}

func ActionStatus(c *gin.Context) {
	if component.Activities == nil {
		c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "No activities", nil, nil))
		return
	}
	data := make(map[uint64]ActionStatusResponseData)
	for i, activity := range component.Activities.Activities {
		data[i] = ActionStatusResponseData{
			IsConnected: activity.Client != nil,
		}
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "Activities existed", data, nil))
}

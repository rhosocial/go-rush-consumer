package controllerActivity

import "C"
import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"go-rush-consumer/component"
	"golang.org/x/net/context"
	"net/http"
)

type ActivityBody struct {
	ActivityID uint64 `form:"activity_id" validate:"required" json:"activity_id" binding:"required"`
}

func (a *ControllerActivity) ActionStatus(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindQuery(&body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "activity existed", activity.ContextCancelFunc == nil, nil))
}

func (a *ControllerActivity) ActionStart(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	err = activity.Start(context.Background())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, component.NewGenericResponse(c, 1, "failed to start a worker", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "the worker has started", nil, nil))
}

func (a *ControllerActivity) ActionStop(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	err = activity.Stop()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, component.NewGenericResponse(c, 1, "failed to stop a worker", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "the worker stopped", nil, nil))
}

func (a *ControllerActivity) ActionAdd(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	if component.Activities.ActivityExists(body.ActivityID) {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity exists", nil, nil))
		return
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "Controller Activity, Action Add.", nil, nil))
}

func (a *ControllerActivity) ActionDelete(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	activity.ContextCancelFuncRWLock.Lock()
	defer activity.ContextCancelFuncRWLock.Unlock()
	if activity.ContextCancelFunc != nil {
		c.AbortWithStatusJSON(http.StatusForbidden, component.NewGenericResponse(c, 1, "activity is working", err.Error(), nil))
		return
	}
	err = component.Activities.RemoveActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, component.NewGenericResponse(c, 1, "failed to remove the activity", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, component.NewGenericResponse(c, 0, "activity removed", nil, nil))
}

type ControllerActivity struct {
	component.GenericController
}

func (c *ControllerActivity) RegisterActions(r *gin.Engine) {
	controller := r.Group("/activity")
	{
		controller.PUT("", c.ActionAdd)
		controller.POST("/delete", c.ActionDelete)
		controller.GET("", c.ActionStatus)
		controller.POST("/start", c.ActionStart)
		controller.POST("/stop", c.ActionStop)
	}
}

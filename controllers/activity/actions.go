package controllerActivity

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	commonComponent "github.com/rhosocial/go-rush-common/component"
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
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "activity existed", activity.ContextCancelFunc == nil, nil))
}

func (a *ControllerActivity) ActionStart(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	err = activity.Start(context.Background())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, commonComponent.NewGenericResponse(c, 1, "failed to start a worker", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "the worker has started", nil, nil))
}

func (a *ControllerActivity) ActionStop(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", nil, nil))
		return
	}
	activity, err := component.Activities.GetActivity(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	}
	err = activity.Stop()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, commonComponent.NewGenericResponse(c, 1, "failed to stop a worker", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "the worker stopped", nil, nil))
}

func (a *ControllerActivity) ActionAdd(c *gin.Context) {
	var body ActivityBody
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", err.Error(), nil))
		return
	}
	err = component.Activities.New(body.ActivityID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "failed to add new activity", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "activity added", nil, nil))
}

type ActivityBodyDelete struct {
	ActivityBody
	StopBeforeRemoving bool `form:"stop_before_removing" json:"stop_before_removing,omitempty" default:"false"`
}

func (a *ControllerActivity) ActionDelete(c *gin.Context) {
	var body ActivityBodyDelete
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", err.Error(), nil))
		return
	}
	err = component.Activities.Remove(body.ActivityID, body.StopBeforeRemoving)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "failed to remove the activity", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "activity removed", nil, nil))
}

type ControllerActivity struct {
	commonComponent.GenericController
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

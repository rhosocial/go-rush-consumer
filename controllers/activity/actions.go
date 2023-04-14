package controllerActivity

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-consumer/component"
	"golang.org/x/net/context"
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
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "activity existed", !activity.IsWorking(), nil))
}

type ActivityBodyAdd struct {
	ActivityBody
	RedisServerIndex *uint8 `form:"redis_server_index" json:"redis_server_index" default:"0"` // 指针表示可以不提供，不提供时按默认值default。
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
	err = activity.Stop(component.ErrWorkerStopped)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, commonComponent.NewGenericResponse(c, 1, "failed to stop a worker", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "the worker stopped", nil, nil))
}

func (a *ControllerActivity) ActionAdd(c *gin.Context) {
	var body ActivityBodyAdd
	err := c.ShouldBindWith(&body, binding.FormPost)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", err.Error(), nil))
		return
	}
	err = component.Activities.New(body.ActivityID, body.RedisServerIndex)
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
	activityID, err := strconv.ParseUint(c.Param("activityID"), 10, 64)
	println(c.Param("activityID"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "activity not valid", err.Error(), nil))
		return
	}
	println(c.Param("stopBeforeRemoving"))
	stopBeforeRemoving, err := strconv.ParseBool(c.Param("stopBeforeRemoving"))
	if err != nil {
		stopBeforeRemoving = false
	}
	err = component.Activities.Remove(activityID, stopBeforeRemoving)
	if err == component.ErrActivityNotExist {
		c.AbortWithStatusJSON(http.StatusNotFound, commonComponent.NewGenericResponse(c, 1, "activity not found", err.Error(), nil))
		return
	} else if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, commonComponent.NewGenericResponse(c, 1, "failed to remove the activity", err.Error(), nil))
		return
	}
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "activity removed", nil, nil))
}

func (a *ControllerActivity) ActionStopAll(c *gin.Context) {
	count := component.Activities.StopAll()
	c.JSON(http.StatusOK, commonComponent.NewGenericResponse(c, 0, "all the workers stopped.", count, nil))
}

type ControllerActivity struct {
	commonComponent.GenericController
}

func (c *ControllerActivity) RegisterActions(r *gin.Engine) {
	controller := r.Group("/activity")
	{
		controller.PUT("", c.ActionAdd)
		controller.DELETE("/:activityID", c.ActionDelete)
		controller.DELETE("/:activityID/:stopBeforeRemoving", c.ActionDelete)
		controller.GET("", c.ActionStatus)
		controller.POST("/start", c.ActionStart)
		controller.POST("/stop", c.ActionStop)
		controller.POST("/stop-all", c.ActionStopAll)
	}
}

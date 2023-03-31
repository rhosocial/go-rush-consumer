package component

import (
	"github.com/gin-gonic/gin"
)

type GenericResponse struct {
	RequestID uint32 `json:"request_id"`
	Code      uint32 `json:"code"`
	Message   string `json:"message"`
	Data      any    `json:"data,omitempty"`
	Extension any    `json:"ext,omitempty"`
}

func NewGenericResponse(c *gin.Context, code uint32, message string, data any, extension any) *GenericResponse {
	r := GenericResponse{
		RequestID: c.Value(ContextRequestID).(uint32),
		Code:      code,
		Message:   message,
		Data:      data,
		Extension: extension,
	}
	return &r
}

type ControllerInterface interface {
	RegisterActions(r *gin.Engine)
}

type GenericController struct {
	ControllerInterface
}

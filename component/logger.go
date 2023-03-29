package component

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"sync/atomic"
	"time"
)

// defaultLogFormatter is the default log format function Logger middleware uses.
var defaultLogFormatter = func(param gin.LogFormatterParams) string {
	var statusColor, methodColor, resetColor string
	if param.IsOutputColor() {
		statusColor = param.StatusCodeColor()
		methodColor = param.MethodColor()
		resetColor = param.ResetColor()
	}

	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}

	return fmt.Sprintf("[RUSH-CONSUMER] %v - %10d |%s %3d %s| %13v | %39s |%s %-7s %s %#v\n%s",
		param.TimeStamp.Format("2006/01/02 - 15:04:05"),
		param.Keys[RequestID],
		statusColor, param.StatusCode, resetColor,
		param.Latency,
		param.ClientIP,
		methodColor, param.Method, resetColor,
		param.Path,
		param.ErrorMessage,
	)
}

func LogFormatter(param gin.LogFormatterParams) string {
	return defaultLogFormatter(param)
}

var requestIDIndex atomic.Int64

const (
	RequestID = "RequestID"
)

// NewRequestID 返回一个新的请求ID。
func NewRequestID(c *gin.Context) {
	usec := time.Now().UnixNano() + requestIDIndex.Add(1)
	requestID := uint32(usec&0x7FFFFFFF | 0x80000000)
	c.Set(RequestID, requestID)
	c.Next()
}

func AppendRequestID() gin.HandlerFunc {
	return NewRequestID
}

package component

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// 定义一个中间件，用于捕获错误并统一返回
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 使用 defer 来捕获 panic
		defer func() {
			if err := recover(); err != nil {
				// 构造一个错误响应
				response := GenericResponse{
					Code:    http.StatusInternalServerError,
					Message: http.StatusText(http.StatusInternalServerError),
				}

				// 返回错误响应
				c.JSON(http.StatusInternalServerError, response)
			}
		}()

		// 继续处理请求
		c.Next()
	}
}

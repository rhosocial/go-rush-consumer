package component

import (
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
	"net/http"
)

func ValidatePassword(passHash string, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(passHash), []byte(password)) == nil
}

func AuthRequired() gin.HandlerFunc {
	return authHandlerFunc
}

type HeaderAuthorizationToken struct {
	AuthorizationToken string `header:"X-Authorization-Token" binding:"required"`
}

func (a *HeaderAuthorizationToken) validate() bool {
	return ValidatePassword(a.AuthorizationToken, "password")
}

func authHandlerFunc(c *gin.Context) {
	h := HeaderAuthorizationToken{}
	err := c.ShouldBindHeader(&h)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusForbidden, GenericResponse{Code: 1, Message: "empty authorization token"})
	} else if !h.validate() {
		c.AbortWithStatusJSON(http.StatusForbidden, GenericResponse{Code: 1, Message: "invalid authorization token"})
	}
	c.Next()
}

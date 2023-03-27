package component

import (
	"golang.org/x/crypto/bcrypt"
	"math/rand"
	"strconv"
	"testing"
)

func TestValidatePassword(t *testing.T) {
	t.Run("Generating 5 password hashes", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			result, _ := bcrypt.GenerateFromPassword([]byte("password"), i+4)
			t.Log(string(result))
		}
	})
	t.Run("Validate Password", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			random := strconv.FormatUint(rand.Uint64(), 10) + strconv.FormatUint(rand.Uint64(), 10)
			result, _ := bcrypt.GenerateFromPassword([]byte(random), i+4)
			if !ValidatePassword(string(result), random) {
				t.Error("Password compared not equal.")
			}
		}
	})
}

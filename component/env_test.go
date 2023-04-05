package component

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	yamlEmpty     = "yamlEmpty*.yaml"
	yamlEmptyFile *os.File
	yamlEmptyDir  string
)

func createTempDirs(t *testing.T) {
	if temp, err := os.MkdirTemp("", "go-rush-consumer-env-*"); err == nil {
		yamlEmptyDir = temp
	} else {
		t.Error(err)
		return
	}
}

func removeTempDirs(t *testing.T) {
	if err := os.RemoveAll(yamlEmptyDir); err != nil {
		t.Log(err)
		return
	}
}

func createTempFiles(t *testing.T) {
	if temp, err := os.CreateTemp(yamlEmptyDir, yamlEmpty); err == nil {
		yamlEmptyFile = temp
	} else {
		t.Error(err)
		return
	}
}

func setupEnv(t *testing.T) {
	createTempDirs(t)
	createTempFiles(t)
}

func teardownEnv(t *testing.T) {
	if yamlEmptyFile != nil {
		if err := yamlEmptyFile.Close(); err != nil {
			t.Log(err)
		}
	}
	removeTempDirs(t)
}

func TestLoadEnvFromYaml_EmptyContent(t *testing.T) {
	setupEnv(t)
	defer teardownEnv(t)

	t.Run("Net", func(t *testing.T) {
		if err := LoadEnvFromYaml(yamlEmptyFile.Name()); err != nil {
			t.Error(err)
			return
		}
		assert.NotNil(t, (*GlobalEnv).Net, "The `Net` attribute of `GlobalEnv` should not be `nil`.")
		assert.NotNil(t, (*(*GlobalEnv).Net).ListenPort, "The `ListenPort` attribute of `Net` should not be `nil`.")
		assert.Equal(t, uint16(80), *(*(*GlobalEnv).Net).ListenPort, "The default port is `80` when not defined.")
	})
}

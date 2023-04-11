package component

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	yamlEmpty     = "default.yaml"
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

func setupEnvFiles(t *testing.T) {
	createTempDirs(t)
	createTempFiles(t)
}

func teardownEnvFiles(t *testing.T) {
	if yamlEmptyFile != nil {
		if err := yamlEmptyFile.Close(); err != nil {
			t.Log(err)
		}
	}
	removeTempDirs(t)
}

func setupEnvVars(t *testing.T) {
	if err := os.Setenv("Net.ListenPort", "8081"); err != nil {
		t.Error(err)
		return
	}
	if err := os.Setenv("Activity.Batch", "127"); err != nil {
		t.Error(err)
		return
	}
}

func teardownEnvVars(t *testing.T) {
	if err := os.Unsetenv("Net.ListenPort"); err != nil {
		t.Error(err)
		return
	}
	if err := os.Unsetenv("Activity.Batch"); err != nil {
		t.Error(err)
		return
	}
}

func TestLoadEnvFromYaml_EmptyContent(t *testing.T) {
	setupEnvFiles(t)
	defer teardownEnvFiles(t)

	t.Run("Net", func(t *testing.T) {
		if err := LoadEnvFromYaml(yamlEmptyFile.Name()); err != nil {
			t.Error(err)
			return
		}
		assert.NotNil(t, (*GlobalEnv).Net, "The `Net` attribute of `GlobalEnv` should not be `nil`.")
		assert.NotNil(t, (*(*GlobalEnv).Net).ListenPort, "The `ListenPort` attribute of `Net` should not be `nil`.")
		assert.Equal(t, uint16(8080), *(*(*GlobalEnv).Net).ListenPort, "The default port is `8080` when not defined.")
	})

	t.Run("RedisServer", func(t *testing.T) {
		if err := LoadEnvFromYaml(yamlEmptyFile.Name()); err != nil {
			t.Error(err)
			return
		}
		assert.NotNil(t, (*GlobalEnv).RedisServers, "The `RedisServer` attribute of `GlobalEnv` should not be `nil`.")
		assert.Len(t, *(*GlobalEnv).RedisServers, 0, "The length of `RedisServer` should be zero.")
	})

	t.Run("Activity", func(t *testing.T) {
		if err := LoadEnvFromYaml(yamlEmptyFile.Name()); err != nil {
			t.Error(err)
			return
		}
		assert.NotNil(t, (*GlobalEnv).Activity, "The `Activity` attribute of `GlobalEnv` should not be `nil`.")
		assert.Equal(t, uint8(100), *(*(*GlobalEnv).Activity).Batch, "The default batch is `100` when not defined.")
	})
}

func TestLoadEnvFromYaml_WrongContent(t *testing.T) {
	setupEnvFiles(t)
	defer teardownEnvFiles(t)
	_, err := yamlEmptyFile.WriteString("wrong content")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = yamlEmptyFile.Seek(0, 0)
	if err != nil {
		t.Error(err)
		return
	}

	if err := LoadEnvFromYaml(yamlEmptyFile.Name()); err == nil {
		t.Error("Error(s) should be occurred.")
		return
	}
}

func TestLoadEnvFromSystemEnvVar(t *testing.T) {
	if err := LoadEnvFromSystemEnvVar(); err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, uint16(8080), *(*(*GlobalEnv).Net).ListenPort)
	assert.Equal(t, uint8(100), *(*(*GlobalEnv).Activity).Batch)

	_, exist := os.LookupEnv("Net.ListenPort")
	assert.False(t, exist)
	_, exist = os.LookupEnv("Activity.Batch")
	assert.False(t, exist)
	setupEnvVars(t)
	defer teardownEnvVars(t)
	port, exist := os.LookupEnv("Net.ListenPort")
	assert.True(t, exist)
	assert.Equal(t, "8081", port)
	batch, exist := os.LookupEnv("Activity.Batch")
	assert.True(t, exist)
	assert.Equal(t, "127", batch)

	if err := LoadEnvFromSystemEnvVar(); err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, uint16(8081), *(*(*GlobalEnv).Net).ListenPort)
	assert.Equal(t, uint8(127), *(*(*GlobalEnv).Activity).Batch)
}

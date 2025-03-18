package testhelpers

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/kndndrj/nvim-dbee/dbee/adapters"
	"github.com/kndndrj/nvim-dbee/dbee/core"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type DuckDBContainer struct {
	tc.Container
	ConnURL string
	Driver  *core.Connection
	TempDir string
}

// NewDuckDBContainer creates a new duckdb container with
// default adapter and connection. The params.URL is overwritten.
// It uses a temporary directory (usually the test suite tempDir) to store the db file.
// The tmpDir is then mounted to the container and all the dependencies are installed
// in the container file, while still being able to connect to the db file in the host.
func NewDuckDBContainer(ctx context.Context, params *core.ConnectionParams, tmpDir string) (*DuckDBContainer, error) {
	seedFile, err := GetTestDataFile("duckdb_seed.sql")
	if err != nil {
		return nil, err
	}

	dbName, containerDBPath := "test.db", "/container/db"
	entrypointCmd := []string{
		"apt-get update",
		"apt-get install -y unzip curl",
		"curl -sL https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-aarch64.zip | funzip > /usr/local/bin/duckdb",
		"chmod +x /usr/local/bin/duckdb",
		fmt.Sprintf("duckdb %s/%s < %s", containerDBPath, dbName, seedFile.Name()),
		"echo 'ready'",
		"tail -f /dev/null", // hack to keep the container running indefinitely
	}

	req := tc.ContainerRequest{
		Image: "debian:12-slim",
		Files: []tc.ContainerFile{
			{
				Reader:            seedFile,
				ContainerFilePath: seedFile.Name(),
				FileMode:          0o755,
			},
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, fmt.Sprintf("%s:%s", tmpDir, containerDBPath))
		},
		Cmd:        []string{"sh", "-c", strings.Join(entrypointCmd, " && ")},
		WaitingFor: wait.ForLog("ready").WithStartupTimeout(10 * time.Second),
	}

	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		ProviderType:     GetContainerProvider(),
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	lg, err := ctr.Logs(ctx)
	if err != nil {
		return nil, err
	}

	lgbytes, err := io.ReadAll(lg)
	if err != nil {
		return nil, err
	}
	fmt.Printf("string(lgbytes): %v\n", string(lgbytes))

	if params.Type == "" {
		params.Type = "duckdb"
	}

	connURL := filepath.Join(tmpDir, dbName)
	if params.URL == "" {
		params.URL = connURL
	}

	driver, err := adapters.NewConnection(params)
	if err != nil {
		return nil, err
	}

	return &DuckDBContainer{
		Container: ctr,
		ConnURL:   connURL,
		Driver:    driver,
		TempDir:   tmpDir,
	}, nil
}

// NewDriver helper function to create a new driver with the connection URL.
func (p *DuckDBContainer) NewDriver(params *core.ConnectionParams) (*core.Connection, error) {
	if params.URL == "" {
		params.URL = p.ConnURL
	}
	if params.Type == "" {
		params.Type = "duckdb"
	}

	return adapters.NewConnection(params)
}

package integration

import (
	"context"
	"log"
	"testing"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	th "github.com/kndndrj/nvim-dbee/dbee/tests/testhelpers"
	"github.com/stretchr/testify/assert"
	tsuite "github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
)

// DuckDBTestSuite is the test suite for the duckdb adapter.
type DuckDBTestSuite struct {
	tsuite.Suite
	ctr *th.DuckDBContainer
	ctx context.Context
	d   *core.Connection
}

func TestDuckDBTestSuite(t *testing.T) {
	tsuite.Run(t, new(DuckDBTestSuite))
}

func (suite *DuckDBTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	tempDir := suite.T().TempDir()

	params := &core.ConnectionParams{ID: "test-duckdb", Name: "test-duckdb"}
	ctr, err := th.NewDuckDBContainer(suite.ctx, params, tempDir)
	if err != nil {
		log.Fatal(err)
	}

	suite.ctr, suite.d = ctr, ctr.Driver
}

func (suite *DuckDBTestSuite) TeardownSuite() {
	tc.CleanupContainer(suite.T(), suite.ctr)
}

func (suite *DuckDBTestSuite) TestShouldErrorInvalidQuery() {
	t := suite.T()

	want := "syntax error"

	call := suite.d.Execute("invalid sql", func(cs core.CallState, c *core.Call) {
		if cs == core.CallStateExecutingFailed {
			assert.ErrorContains(t, c.Err(), want)
		}
	})
	assert.NotNil(t, call)
}

func (suite *DuckDBTestSuite) TestShouldCancelQuery() {
	t := suite.T()
	want := []core.CallState{core.CallStateExecuting, core.CallStateCanceled}

	_, got, err := th.GetResultWithCancel(t, suite.d, "SELECT 1")
	assert.NoError(t, err)

	assert.Equal(t, want, got)
}

func (suite *DuckDBTestSuite) TestShouldReturnManyRows() {
	t := suite.T()

	wantStates := []core.CallState{
		core.CallStateExecuting, core.CallStateRetrieving, core.CallStateArchived,
	}
	wantCols := []string{"id", "username"}
	wantRows := []core.Row{
		{int64(1), "john_doe"},
		{int64(2), "jane_smith"},
		{int64(3), "bob_wilson"},
	}

	query := "SELECT id, username FROM test_table"

	gotRows, gotCols, gotStates, err := th.GetResult(t, suite.d, query)
	assert.NoError(t, err)

	assert.ElementsMatch(t, wantCols, gotCols)
	assert.ElementsMatch(t, wantStates, gotStates)
	assert.Equal(t, wantRows, gotRows)
}

func (suite *DuckDBTestSuite) TestShouldReturnOneRow() {
	t := suite.T()

	wantStates := []core.CallState{
		core.CallStateExecuting, core.CallStateRetrieving, core.CallStateArchived,
	}
	wantCols := []string{"id", "username"}
	wantRows := []core.Row{{int64(2), "jane_smith"}}

	query := "SELECT id, username FROM test_view"

	gotRows, gotCols, gotStates, err := th.GetResult(t, suite.d, query)
	assert.NoError(t, err)

	assert.ElementsMatch(t, wantCols, gotCols)
	assert.ElementsMatch(t, wantStates, gotStates)
	assert.Equal(t, wantRows, gotRows)
}

func (suite *DuckDBTestSuite) TestShouldReturnStructure() {
	t := suite.T()

	var (
		wantSchema    = "duckdb_schema"
		wantSomeTable = "test_table"
		wantSomeView  = "test_view"
	)

	structure, err := suite.d.GetStructure()
	assert.NoError(t, err)

	gotSchemas := th.GetSchemas(t, structure)
	assert.Contains(t, gotSchemas, wantSchema)

	gotTables := th.GetModels(t, structure, core.StructureTypeTable)
	assert.Contains(t, gotTables, wantSomeTable)

	gotViews := th.GetModels(t, structure, core.StructureTypeView)
	assert.Contains(t, gotViews, wantSomeView)
}

func (suite *DuckDBTestSuite) TestShouldReturnColumns() {
	t := suite.T()

	want := []*core.Column{
		{Name: "id", Type: "INTEGER"},
		{Name: "username", Type: "TEXT"},
		{Name: "email", Type: "TEXT"},
	}

	got, err := suite.d.GetColumns(&core.TableOptions{
		Table:           "test_table",
		Schema:          "duckdb_schema",
		Materialization: core.StructureTypeTable,
	})

	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func (suite *DuckDBTestSuite) TestShouldNoOperationSwitchDatabase() {
	t := suite.T()

	driver, err := suite.ctr.NewDriver(&core.ConnectionParams{
		ID:   "test-duckdb-2",
		Name: "test-duckdb-2",
	})
	assert.NoError(t, err)

	err = driver.SelectDatabase("no-op")
	assert.Nil(t, err)
}

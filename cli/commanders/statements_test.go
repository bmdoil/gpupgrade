package commanders_test

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/greenplum-db/gpupgrade/cli/commanders"
	"github.com/greenplum-db/gpupgrade/utils"
)

func TestIndexStatement(t *testing.T) {
	t.Run("FQN", func(t *testing.T) {
		index := commanders.IndexStatement{Schema: "public", Name: "index1"}
		expected := "public.index1"
		if got := index.FQN(); got != expected {
			t.Errorf("FQN() = %v, want %v", got, expected)
		}
	})

	t.Run("Write", func(t *testing.T) {
		expected := "-- TYPE: INDEX, SCHEMA: public, NAME: index1, TABLE: table1\nCREATE INDEX index1 ON table1 (column1);\n-- END\n"
		index := commanders.IndexStatement{Schema: "public", Name: "index1", Table: "table1", Definition: "CREATE INDEX index1 ON table1 (column1);"}
		buf := new(bytes.Buffer)
		err := index.Write(buf)
		if err != nil {
			t.Errorf("IndexStatement.Write() unexpected error: %v", err)
		}
		if got := buf.String(); got != expected {
			t.Errorf("IndexStatement.Write() = %v, want %v", got, expected)
		}
	})

	t.Run("Parse", func(t *testing.T) {
		index := commanders.IndexStatement{}
		stmt := "-- TYPE: INDEX, SCHEMA: public, NAME: index1, TABLE: table1\nCREATE INDEX index1 ON table1 (column1);"
		err := index.Parse(stmt)
		if err != nil {
			t.Errorf("IndexStatement.Parse() unexpected error: %v", err)
		}
		if index.Schema != "public" || index.Name != "index1" || index.Table != "table1" || index.Definition != "CREATE INDEX index1 ON table1 (column1);" {
			t.Errorf("IndexStatement.Parse() = %v, want %v", index, commanders.IndexStatement{Schema: "public", Name: "index1", Table: "table1", Definition: "CREATE INDEX index1 ON table1 (column1);"})
		}
	})

	t.Run("IndexSplitFunc", func(t *testing.T) {
		data := []byte("-- TYPE: INDEX, SCHEMA: public, NAME: index1, TABLE: table1\nCREATE INDEX index1 ON table1 (column1);\n-- END\n-- TYPE: INDEX, SCHEMA: public, NAME: index2, TABLE: table2\nCREATE INDEX index2 ON table2 (column2);")
		expectedStmt1 := "-- TYPE: INDEX, SCHEMA: public, NAME: index1, TABLE: table1\nCREATE INDEX index1 ON table1 (column1);"
		expectedStmt2 := "-- TYPE: INDEX, SCHEMA: public, NAME: index2, TABLE: table2\nCREATE INDEX index2 ON table2 (column2);"

		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		scanner.Split(commanders.IndexSplitFunc)
		var results []string
		for scanner.Scan() {
			results = append(results, scanner.Text())
		}
		if len(results) != 2 {
			t.Errorf("IndexSplitFunc() = \n%v\n want \n%v\n", len(results), 2)
		}
		if results[0] != expectedStmt1 {
			t.Errorf("IndexSplitFunc() = \n%v\n want \n%v\n", results[0], expectedStmt1)
		}
		if results[1] != expectedStmt2 {
			t.Errorf("IndexSplitFunc() = \n%v\n want \n%v\n", results[1], expectedStmt2)
		}
	})
}

func TestReadIndexStatements(t *testing.T) {
	utils.System.ReadFile = func(filename string) ([]byte, error) {
		if filename != "filename" {
			t.Errorf("ReadFile() filename = %v, want %v", filename, "filename")
		}
		return []byte("\\c postgres\n-- TYPE: INDEX, SCHEMA: public, NAME: index1, TABLE: table1\nCREATE INDEX index1 ON table1 (column1);\n-- END\n-- TYPE: INDEX, SCHEMA: public, NAME: index2, TABLE: table2\nCREATE INDEX index2 ON table2 (column2);\n-- END\n"), nil
	}
	defer utils.ResetSystemFunctions()

	indexStatements, err := commanders.ReadIndexStatements("filename")
	if err != nil {
		t.Errorf("ReadIndexStatements() unexpected error: %v", err)
	}

	if len(indexStatements.Statements) != 2 {
		t.Errorf("ReadIndexStatements() = %v, want %v", len(indexStatements.Statements), 2)
	}

	expectedStmt1 := commanders.IndexStatement{Schema: "public", Name: "index1", Table: "table1", Definition: "CREATE INDEX index1 ON table1 (column1);"}
	expectedStmt2 := commanders.IndexStatement{Schema: "public", Name: "index2", Table: "table2", Definition: "CREATE INDEX index2 ON table2 (column2);"}

	if indexStatements.Statements[0] != expectedStmt1 {
		t.Errorf("ReadIndexStatements() = %v, want %v", indexStatements.Statements[0], expectedStmt1)
	}
	if indexStatements.Statements[1] != expectedStmt2 {
		t.Errorf("ReadIndexStatements() = %v, want %v", indexStatements.Statements[1], expectedStmt2)
	}
}

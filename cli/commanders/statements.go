package commanders

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"

	"github.com/greenplum-db/gpupgrade/utils"
)

type Statement interface {
	FQN() string
	Write(outputFile string) error
	Parse(stmt string) error
}

type Statements []Statement

type IndexStatements struct {
	Database string
	Filename string
	Statements []IndexStatement
}

type IndexStatement struct {
	Schema string
	Name string
	Table string
	Definition string
}

func (i *IndexStatement) FQN() string {
	return fmt.Sprintf("%s.%s", i.Schema, i.Name)
}

func (i *IndexStatement) Write(buf *bytes.Buffer) (error) {
	var err error
	headerOutput := fmt.Sprintf("-- TYPE: INDEX, SCHEMA: %s, NAME: %s, TABLE: %s\n", i.Schema, i.Name, i.Table)
	defOutput := fmt.Sprintf("%s\n", i.Definition)
	footerOutput := "-- END\n"
	_, err = buf.WriteString(headerOutput)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(defOutput)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(footerOutput)
	if err != nil {
		return err
	}

	return nil
}

func (i *IndexStatement) Parse(stmt string) error {
	lines := strings.Split(stmt, "\n")
	header := lines[0]
	parts := strings.Split(header, ", ")

	// Check if the header starts with "-- TYPE: INDEX"
	if !strings.HasPrefix(header, "-- TYPE: INDEX") {
		return fmt.Errorf("invalid statement: expected '-- TYPE: INDEX'")
	}

	schema := strings.TrimPrefix(parts[1], "SCHEMA: ")
	name := strings.TrimPrefix(parts[2], "NAME: ")
	table := strings.TrimPrefix(parts[3], "TABLE: ")

	// The definition starts from the second line and ends before the end token
	definition := strings.Join(lines[1:], "\n")

	i.Schema = strings.TrimSpace(schema)
	i.Name = strings.TrimSpace(name)
	i.Table = strings.TrimSpace(table)
	i.Definition = definition

	return nil
}

func ReadIndexStatements(inputFile string) (IndexStatements, error) {
	indexStatements := IndexStatements{}
	statementSlice := make([]IndexStatement, 0)
	contents, err := utils.System.ReadFile(inputFile)
	if err != nil {
		return indexStatements, err
	}

	indexStatements.Filename = inputFile
	indexStatements.Database = readDatabaseNameFromScript(contents)
	if indexStatements.Database == "" {
		return indexStatements, fmt.Errorf("could not determine database name from file %s", inputFile)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(contents)))

	scanner.Split(IndexSplitFunc)
	for scanner.Scan() {
		index := IndexStatement{}
		stmt := scanner.Text()
		err := index.Parse(stmt)
		if err != nil {
			return indexStatements, err
		}

		statementSlice = append(statementSlice, index)
	}

	indexStatements.Statements = statementSlice

	return indexStatements, nil
}

func IndexSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Find the "-- TYPE: INDEX" token
	if i := bytes.Index(data, []byte("-- TYPE: INDEX")); i >= 0 {
		// Find the "-- END" token
		if j := bytes.Index(data[i:], []byte("\n-- END\n")); j >= 0 {
				// We have a complete IndexStatement
				return i + j + len("\n-- END\n"), data[i : i+j], nil
		}
	}

	// If at EOF, return remainder as a token
	if atEOF && len(data) > 0 {
			return len(data), data, nil
	}

	return 0, nil, nil
}

// Parses \c command from the script to determine the database name
func readDatabaseNameFromScript(b []byte) string {
	s := string(b)
	s = strings.Split(s, "\n")[0]
	if strings.HasPrefix(s, "\\c") {
		return strings.Split(s, " ")[1]
	}
	return ""
}

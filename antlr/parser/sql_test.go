package parser

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"testing"
)

// Define a custom listener that will print the parse tree nodes
type ParseTreePrinter struct {
	*BaseSqlListener
}

// NewParseTreePrinter creates a new instance of ParseTreePrinter
func NewParseTreePrinter() *ParseTreePrinter {
	return &ParseTreePrinter{}
}

// Override the EnterQuery method to print when entering a query rule
func (p *ParseTreePrinter) EnterQuery(ctx *QueryContext) {
	fmt.Printf("Entering query: %s\n", ctx.GetText())
}

// Override the EnterSelectStatement method to print when entering a selectStatement rule
func (p *ParseTreePrinter) EnterSelectStatement(ctx *SelectStatementContext) {
	fmt.Printf("Entering selectStatement: %s\n", ctx.GetText())
}

// Override the EnterColumnList method to print when entering a columnList rule
func (p *ParseTreePrinter) EnterColumnList(ctx *ColumnListContext) {
	fmt.Printf("Entering columnList: %s\n", ctx.GetText())
}

// Override the EnterColumnName method to print when entering a columnName rule
func (p *ParseTreePrinter) EnterColumnName(ctx *ColumnNameContext) {
	fmt.Printf("Entering columnName: %s\n", ctx.GetText())
}

// Override the EnterTableName method to print when entering a tableName rule
func (p *ParseTreePrinter) EnterTableName(ctx *TableNameContext) {
	fmt.Printf("Entering tableName: %s\n", ctx.GetText())
}

// TestParseTree demonstrates creating and walking through a parse tree
func TestParseSQLTree(t *testing.T) {
	input := `SELECT "my column name", column_name FROM my_table WHERE col2 = col7`

	// Create the input stream
	is := antlr.NewInputStream(input)

	// Create the lexer
	lexer := NewSqlLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create the parser
	parser := NewSqlParser(stream)

	// Build the parse tree
	tree := parser.Query()

	// Create a parse tree walker
	walker := antlr.ParseTreeWalkerDefault

	// Create and attach the listener
	listener := NewParseTreePrinter()
	walker.Walk(listener, tree)
}

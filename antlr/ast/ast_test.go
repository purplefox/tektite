package ast

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/spirit-labs/tektite/antlr/parser"
	"testing"
)

func TestAst(t *testing.T) {
	// Example usage
	input := "SELECT column1, 42, 'example' FROM table_name WHERE column1 > 10 AND (column2 = 'value' OR column3 < 3.14)"

	// Setup the ANTLR input stream and lexer
	is := antlr.NewInputStream(input)
	lexer := parser.NewSqlLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Setup the parser
	p := parser.NewSqlParser(stream)
	tree := p.Query() // Parse the input to get the parse tree

	// Walk the parse tree to build the AST
	var visitor parser.SqlVisitor
	visitor = parser.SqlVisitor(&SQLASTBuilderVisitor{})

	ast := visitor.Visit(tree)

	// Output the AST
	fmt.Printf("%+v\n", ast)
}

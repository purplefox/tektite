package ast

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/spirit-labs/tektite/antlr/parser"
	"strconv"
)

// Represents a SQL SELECT statement
type SelectStatement struct {
	Columns []ColumnExpr // List of column expressions in the SELECT clause
	Table   string       // Table name in the FROM clause
	Where   *WhereClause // Optional WHERE clause
}

// Represents a column expression in the SELECT clause
type ColumnExpr struct {
	Name     *string       // Column name
	Literal  *LiteralValue // Literal value (e.g., string, int, float, bool)
	Function *FunctionExpr // Function expression (e.g., COUNT(*))
}

// Represents a literal value (e.g., a string, int, float, or boolean)
type LiteralValue struct {
	StringValue *string  // String literal
	IntValue    *int     // Integer literal
	FloatValue  *float64 // Float literal
	BoolValue   *bool    // Boolean literal
}

// Represents a function expression (e.g., COUNT(column))
type FunctionExpr struct {
	Name   string       // Name of the function (e.g., COUNT)
	Params []ColumnExpr // List of parameters to the function
}

// Represents a WHERE clause with a condition expression
type WhereClause struct {
	Condition Expr // The condition expression in the WHERE clause
}

// Represents a generic expression in the WHERE clause
type Expr interface {
	IsExpr()
}

// Represents a comparison expression (e.g., column1 > 10)
type ComparisonExpr struct {
	Left     ColumnExpr // Left-hand side of the comparison
	Operator string     // Comparison operator (e.g., =, !=, <, >)
	Right    ColumnExpr // Right-hand side of the comparison
}

// Represents a logical AND or OR expression
type LogicalExpr struct {
	Left     Expr   // Left-hand side expression
	Operator string // Logical operator (AND, OR)
	Right    Expr   // Right-hand side expression
}

// Represents a NOT expression
type NotExpr struct {
	Expr Expr // The expression being negated
}

// Represents a parenthesized expression
type ParenExpr struct {
	Expr Expr // The expression inside the parentheses
}

// Implement the IsExpr method for each expression type to satisfy the Expr interface
func (*ComparisonExpr) IsExpr() {}
func (*LogicalExpr) IsExpr()    {}
func (*NotExpr) IsExpr()        {}
func (*ParenExpr) IsExpr()      {}

var _ parser.SqlVisitor = &SQLASTBuilderVisitor{}

// SQLASTBuilderVisitor is a custom visitor to build the AST
type SQLASTBuilderVisitor struct {
}

func (v *SQLASTBuilderVisitor) VisitQuery(ctx *parser.QueryContext) interface{} {
	return v.VisitSelectStatement(ctx.SelectStatement().(*parser.SelectStatementContext))
}

// VisitSelectStatement handles the SELECT statement node
func (v *SQLASTBuilderVisitor) VisitSelectStatement(ctx *parser.SelectStatementContext) interface{} {
	columns := v.VisitColumnList(ctx.SelectClause().ColumnList().(*parser.ColumnListContext)).([]ColumnExpr)
	table := ctx.FromClause().TableName().GetText()
	var whereClause *WhereClause
	if ctx.WhereClause() != nil {
		whereClause = v.Visit(ctx.WhereClause()).(*WhereClause)
	}
	return &SelectStatement{
		Columns: columns,
		Table:   table,
		Where:   whereClause,
	}
}

func (v *SQLASTBuilderVisitor) VisitColumnList(ctx *parser.ColumnListContext) interface{} {
	var columns []ColumnExpr
	for _, exprCtx := range ctx.AllColumnExpr() {
		columns = append(columns, v.VisitColumnExpr(exprCtx.(*parser.ColumnExprContext)).(ColumnExpr))
	}
	return columns
}

// VisitColumnExpr handles individual column expressions
func (v *SQLASTBuilderVisitor) VisitColumnExpr(ctx *parser.ColumnExprContext) interface{} {
	if ctx.ColumnName() != nil {
		// It's a column name
		name := ctx.ColumnName().GetText()
		return ColumnExpr{Name: &name}
	} else if ctx.StringLiteral() != nil {
		// It's a string literal
		value := ctx.StringLiteral().GetText()
		return ColumnExpr{Literal: &LiteralValue{StringValue: &value}}
	} else if ctx.IntLiteral() != nil {
		// It's an int literal
		value := intFromText(ctx.IntLiteral().GetText())
		return ColumnExpr{Literal: &LiteralValue{IntValue: &value}}
	} else if ctx.FloatLiteral() != nil {
		// It's a float literal
		value := floatFromText(ctx.FloatLiteral().GetText())
		return ColumnExpr{Literal: &LiteralValue{FloatValue: &value}}
	} else if ctx.BoolLiteral() != nil {
		// It's a boolean literal
		value := boolFromText(ctx.BoolLiteral().GetText())
		return ColumnExpr{Literal: &LiteralValue{BoolValue: &value}}
	} else if ctx.FunctionExpr() != nil {
		// It's a function expression
		return v.Visit(ctx.FunctionExpr()).(ColumnExpr)
	}
	return nil
}

// VisitWhereClause handles the WHERE clause
func (v *SQLASTBuilderVisitor) VisitWhereClause(ctx *parser.WhereClauseContext) interface{} {
	return &WhereClause{
		Condition: v.Visit(ctx.ConditionExpr()).(Expr),
	}
}

// VisitComparisonCondition handles comparison expressions in the WHERE clause
func (v *SQLASTBuilderVisitor) VisitComparisonCondition(ctx *parser.ComparisonConditionContext) interface{} {
	left := v.Visit(ctx.ColumnExpr(0)).(ColumnExpr)
	operator := ctx.ComparisonOperator().GetText()
	right := v.Visit(ctx.ColumnExpr(1)).(ColumnExpr)
	return &ComparisonExpr{
		Left:     left,
		Operator: operator,
		Right:    right,
	}
}

// VisitAndCondition and VisitOrCondition handle AND and OR expressions
func (v *SQLASTBuilderVisitor) VisitAndCondition(ctx *parser.AndConditionContext) interface{} {
	left := v.Visit(ctx.ConditionExpr(0)).(Expr)
	right := v.Visit(ctx.ConditionExpr(1)).(Expr)
	return &LogicalExpr{
		Left:     left,
		Operator: "AND",
		Right:    right,
	}
}

func (v *SQLASTBuilderVisitor) VisitOrCondition(ctx *parser.OrConditionContext) interface{} {
	left := v.Visit(ctx.ConditionExpr(0)).(Expr)
	right := v.Visit(ctx.ConditionExpr(1)).(Expr)
	return &LogicalExpr{
		Left:     left,
		Operator: "OR",
		Right:    right,
	}
}

// VisitNotCondition handles NOT expressions
func (v *SQLASTBuilderVisitor) VisitNotCondition(ctx *parser.NotConditionContext) interface{} {
	expr := v.Visit(ctx.ConditionExpr()).(Expr)
	return &NotExpr{Expr: expr}
}

// VisitParenthesizedCondition handles parenthesized expressions
func (v *SQLASTBuilderVisitor) VisitParenthesizedCondition(ctx *parser.ParenthesizedConditionContext) interface{} {
	expr := v.Visit(ctx.ConditionExpr()).(Expr)
	return &ParenExpr{Expr: expr}
}

// Helper functions to convert text to appropriate types
func intFromText(text string) int {
	// Implement conversion logic here
	i, err := strconv.Atoi(text)
	if err != nil {
		panic(err)
	}
	return i
}

func floatFromText(text string) float64 {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func boolFromText(text string) bool {
	b, err := strconv.ParseBool(text)
	if err != nil {
		panic(err)
	}
	return b
}

func (v *SQLASTBuilderVisitor) Visit(tree antlr.ParseTree) interface{} {
	return tree.Accept(v)
}

func (v *SQLASTBuilderVisitor) VisitChildren(node antlr.RuleNode) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitTerminal(node antlr.TerminalNode) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitErrorNode(node antlr.ErrorNode) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitSelectClause(ctx *parser.SelectClauseContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitFromClause(ctx *parser.FromClauseContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitColumnName(ctx *parser.ColumnNameContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitTableName(ctx *parser.TableNameContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitComparisonOperator(ctx *parser.ComparisonOperatorContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitStringLiteral(ctx *parser.StringLiteralContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitIntLiteral(ctx *parser.IntLiteralContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitFloatLiteral(ctx *parser.FloatLiteralContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitBoolLiteral(ctx *parser.BoolLiteralContext) interface{} {
	return nil
}

func (v *SQLASTBuilderVisitor) VisitFunctionExpr(ctx *parser.FunctionExprContext) interface{} {
	return nil
}

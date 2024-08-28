// Code generated from Sql.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Sql

import "github.com/antlr4-go/antlr/v4"

// SqlListener is a complete listener for a parse tree produced by SqlParser.
type SqlListener interface {
	antlr.ParseTreeListener

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterSelectStatement is called when entering the selectStatement production.
	EnterSelectStatement(c *SelectStatementContext)

	// EnterSelectClause is called when entering the selectClause production.
	EnterSelectClause(c *SelectClauseContext)

	// EnterFromClause is called when entering the fromClause production.
	EnterFromClause(c *FromClauseContext)

	// EnterWhereClause is called when entering the whereClause production.
	EnterWhereClause(c *WhereClauseContext)

	// EnterColumnList is called when entering the columnList production.
	EnterColumnList(c *ColumnListContext)

	// EnterColumnExpr is called when entering the columnExpr production.
	EnterColumnExpr(c *ColumnExprContext)

	// EnterColumnName is called when entering the columnName production.
	EnterColumnName(c *ColumnNameContext)

	// EnterTableName is called when entering the tableName production.
	EnterTableName(c *TableNameContext)

	// EnterOrCondition is called when entering the orCondition production.
	EnterOrCondition(c *OrConditionContext)

	// EnterAndCondition is called when entering the andCondition production.
	EnterAndCondition(c *AndConditionContext)

	// EnterParenthesizedCondition is called when entering the parenthesizedCondition production.
	EnterParenthesizedCondition(c *ParenthesizedConditionContext)

	// EnterNotCondition is called when entering the notCondition production.
	EnterNotCondition(c *NotConditionContext)

	// EnterComparisonCondition is called when entering the comparisonCondition production.
	EnterComparisonCondition(c *ComparisonConditionContext)

	// EnterComparisonOperator is called when entering the comparisonOperator production.
	EnterComparisonOperator(c *ComparisonOperatorContext)

	// EnterStringLiteral is called when entering the stringLiteral production.
	EnterStringLiteral(c *StringLiteralContext)

	// EnterIntLiteral is called when entering the intLiteral production.
	EnterIntLiteral(c *IntLiteralContext)

	// EnterFloatLiteral is called when entering the floatLiteral production.
	EnterFloatLiteral(c *FloatLiteralContext)

	// EnterBoolLiteral is called when entering the boolLiteral production.
	EnterBoolLiteral(c *BoolLiteralContext)

	// EnterFunctionExpr is called when entering the functionExpr production.
	EnterFunctionExpr(c *FunctionExprContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitSelectStatement is called when exiting the selectStatement production.
	ExitSelectStatement(c *SelectStatementContext)

	// ExitSelectClause is called when exiting the selectClause production.
	ExitSelectClause(c *SelectClauseContext)

	// ExitFromClause is called when exiting the fromClause production.
	ExitFromClause(c *FromClauseContext)

	// ExitWhereClause is called when exiting the whereClause production.
	ExitWhereClause(c *WhereClauseContext)

	// ExitColumnList is called when exiting the columnList production.
	ExitColumnList(c *ColumnListContext)

	// ExitColumnExpr is called when exiting the columnExpr production.
	ExitColumnExpr(c *ColumnExprContext)

	// ExitColumnName is called when exiting the columnName production.
	ExitColumnName(c *ColumnNameContext)

	// ExitTableName is called when exiting the tableName production.
	ExitTableName(c *TableNameContext)

	// ExitOrCondition is called when exiting the orCondition production.
	ExitOrCondition(c *OrConditionContext)

	// ExitAndCondition is called when exiting the andCondition production.
	ExitAndCondition(c *AndConditionContext)

	// ExitParenthesizedCondition is called when exiting the parenthesizedCondition production.
	ExitParenthesizedCondition(c *ParenthesizedConditionContext)

	// ExitNotCondition is called when exiting the notCondition production.
	ExitNotCondition(c *NotConditionContext)

	// ExitComparisonCondition is called when exiting the comparisonCondition production.
	ExitComparisonCondition(c *ComparisonConditionContext)

	// ExitComparisonOperator is called when exiting the comparisonOperator production.
	ExitComparisonOperator(c *ComparisonOperatorContext)

	// ExitStringLiteral is called when exiting the stringLiteral production.
	ExitStringLiteral(c *StringLiteralContext)

	// ExitIntLiteral is called when exiting the intLiteral production.
	ExitIntLiteral(c *IntLiteralContext)

	// ExitFloatLiteral is called when exiting the floatLiteral production.
	ExitFloatLiteral(c *FloatLiteralContext)

	// ExitBoolLiteral is called when exiting the boolLiteral production.
	ExitBoolLiteral(c *BoolLiteralContext)

	// ExitFunctionExpr is called when exiting the functionExpr production.
	ExitFunctionExpr(c *FunctionExprContext)
}

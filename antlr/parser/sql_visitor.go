// Code generated from Sql.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Sql

import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by SqlParser.
type SqlVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by SqlParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by SqlParser#selectStatement.
	VisitSelectStatement(ctx *SelectStatementContext) interface{}

	// Visit a parse tree produced by SqlParser#selectClause.
	VisitSelectClause(ctx *SelectClauseContext) interface{}

	// Visit a parse tree produced by SqlParser#fromClause.
	VisitFromClause(ctx *FromClauseContext) interface{}

	// Visit a parse tree produced by SqlParser#whereClause.
	VisitWhereClause(ctx *WhereClauseContext) interface{}

	// Visit a parse tree produced by SqlParser#columnList.
	VisitColumnList(ctx *ColumnListContext) interface{}

	// Visit a parse tree produced by SqlParser#columnExpr.
	VisitColumnExpr(ctx *ColumnExprContext) interface{}

	// Visit a parse tree produced by SqlParser#columnName.
	VisitColumnName(ctx *ColumnNameContext) interface{}

	// Visit a parse tree produced by SqlParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by SqlParser#orCondition.
	VisitOrCondition(ctx *OrConditionContext) interface{}

	// Visit a parse tree produced by SqlParser#andCondition.
	VisitAndCondition(ctx *AndConditionContext) interface{}

	// Visit a parse tree produced by SqlParser#parenthesizedCondition.
	VisitParenthesizedCondition(ctx *ParenthesizedConditionContext) interface{}

	// Visit a parse tree produced by SqlParser#notCondition.
	VisitNotCondition(ctx *NotConditionContext) interface{}

	// Visit a parse tree produced by SqlParser#comparisonCondition.
	VisitComparisonCondition(ctx *ComparisonConditionContext) interface{}

	// Visit a parse tree produced by SqlParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by SqlParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by SqlParser#intLiteral.
	VisitIntLiteral(ctx *IntLiteralContext) interface{}

	// Visit a parse tree produced by SqlParser#floatLiteral.
	VisitFloatLiteral(ctx *FloatLiteralContext) interface{}

	// Visit a parse tree produced by SqlParser#boolLiteral.
	VisitBoolLiteral(ctx *BoolLiteralContext) interface{}

	// Visit a parse tree produced by SqlParser#functionExpr.
	VisitFunctionExpr(ctx *FunctionExprContext) interface{}
}

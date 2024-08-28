// Code generated from Sql.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Sql

import "github.com/antlr4-go/antlr/v4"

// BaseSqlListener is a complete listener for a parse tree produced by SqlParser.
type BaseSqlListener struct{}

var _ SqlListener = &BaseSqlListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseSqlListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseSqlListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseSqlListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseSqlListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseSqlListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseSqlListener) ExitQuery(ctx *QueryContext) {}

// EnterSelectStatement is called when production selectStatement is entered.
func (s *BaseSqlListener) EnterSelectStatement(ctx *SelectStatementContext) {}

// ExitSelectStatement is called when production selectStatement is exited.
func (s *BaseSqlListener) ExitSelectStatement(ctx *SelectStatementContext) {}

// EnterSelectClause is called when production selectClause is entered.
func (s *BaseSqlListener) EnterSelectClause(ctx *SelectClauseContext) {}

// ExitSelectClause is called when production selectClause is exited.
func (s *BaseSqlListener) ExitSelectClause(ctx *SelectClauseContext) {}

// EnterFromClause is called when production fromClause is entered.
func (s *BaseSqlListener) EnterFromClause(ctx *FromClauseContext) {}

// ExitFromClause is called when production fromClause is exited.
func (s *BaseSqlListener) ExitFromClause(ctx *FromClauseContext) {}

// EnterWhereClause is called when production whereClause is entered.
func (s *BaseSqlListener) EnterWhereClause(ctx *WhereClauseContext) {}

// ExitWhereClause is called when production whereClause is exited.
func (s *BaseSqlListener) ExitWhereClause(ctx *WhereClauseContext) {}

// EnterColumnList is called when production columnList is entered.
func (s *BaseSqlListener) EnterColumnList(ctx *ColumnListContext) {}

// ExitColumnList is called when production columnList is exited.
func (s *BaseSqlListener) ExitColumnList(ctx *ColumnListContext) {}

// EnterColumnExpr is called when production columnExpr is entered.
func (s *BaseSqlListener) EnterColumnExpr(ctx *ColumnExprContext) {}

// ExitColumnExpr is called when production columnExpr is exited.
func (s *BaseSqlListener) ExitColumnExpr(ctx *ColumnExprContext) {}

// EnterColumnName is called when production columnName is entered.
func (s *BaseSqlListener) EnterColumnName(ctx *ColumnNameContext) {}

// ExitColumnName is called when production columnName is exited.
func (s *BaseSqlListener) ExitColumnName(ctx *ColumnNameContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseSqlListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseSqlListener) ExitTableName(ctx *TableNameContext) {}

// EnterOrCondition is called when production orCondition is entered.
func (s *BaseSqlListener) EnterOrCondition(ctx *OrConditionContext) {}

// ExitOrCondition is called when production orCondition is exited.
func (s *BaseSqlListener) ExitOrCondition(ctx *OrConditionContext) {}

// EnterAndCondition is called when production andCondition is entered.
func (s *BaseSqlListener) EnterAndCondition(ctx *AndConditionContext) {}

// ExitAndCondition is called when production andCondition is exited.
func (s *BaseSqlListener) ExitAndCondition(ctx *AndConditionContext) {}

// EnterParenthesizedCondition is called when production parenthesizedCondition is entered.
func (s *BaseSqlListener) EnterParenthesizedCondition(ctx *ParenthesizedConditionContext) {}

// ExitParenthesizedCondition is called when production parenthesizedCondition is exited.
func (s *BaseSqlListener) ExitParenthesizedCondition(ctx *ParenthesizedConditionContext) {}

// EnterNotCondition is called when production notCondition is entered.
func (s *BaseSqlListener) EnterNotCondition(ctx *NotConditionContext) {}

// ExitNotCondition is called when production notCondition is exited.
func (s *BaseSqlListener) ExitNotCondition(ctx *NotConditionContext) {}

// EnterComparisonCondition is called when production comparisonCondition is entered.
func (s *BaseSqlListener) EnterComparisonCondition(ctx *ComparisonConditionContext) {}

// ExitComparisonCondition is called when production comparisonCondition is exited.
func (s *BaseSqlListener) ExitComparisonCondition(ctx *ComparisonConditionContext) {}

// EnterComparisonOperator is called when production comparisonOperator is entered.
func (s *BaseSqlListener) EnterComparisonOperator(ctx *ComparisonOperatorContext) {}

// ExitComparisonOperator is called when production comparisonOperator is exited.
func (s *BaseSqlListener) ExitComparisonOperator(ctx *ComparisonOperatorContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseSqlListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseSqlListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterIntLiteral is called when production intLiteral is entered.
func (s *BaseSqlListener) EnterIntLiteral(ctx *IntLiteralContext) {}

// ExitIntLiteral is called when production intLiteral is exited.
func (s *BaseSqlListener) ExitIntLiteral(ctx *IntLiteralContext) {}

// EnterFloatLiteral is called when production floatLiteral is entered.
func (s *BaseSqlListener) EnterFloatLiteral(ctx *FloatLiteralContext) {}

// ExitFloatLiteral is called when production floatLiteral is exited.
func (s *BaseSqlListener) ExitFloatLiteral(ctx *FloatLiteralContext) {}

// EnterBoolLiteral is called when production boolLiteral is entered.
func (s *BaseSqlListener) EnterBoolLiteral(ctx *BoolLiteralContext) {}

// ExitBoolLiteral is called when production boolLiteral is exited.
func (s *BaseSqlListener) ExitBoolLiteral(ctx *BoolLiteralContext) {}

// EnterFunctionExpr is called when production functionExpr is entered.
func (s *BaseSqlListener) EnterFunctionExpr(ctx *FunctionExprContext) {}

// ExitFunctionExpr is called when production functionExpr is exited.
func (s *BaseSqlListener) ExitFunctionExpr(ctx *FunctionExprContext) {}

// Code generated from Sql.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Sql

import "github.com/antlr4-go/antlr/v4"

type BaseSqlVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseSqlVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitSelectStatement(ctx *SelectStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitSelectClause(ctx *SelectClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitFromClause(ctx *FromClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitWhereClause(ctx *WhereClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitColumnList(ctx *ColumnListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitColumnExpr(ctx *ColumnExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitColumnName(ctx *ColumnNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitTableName(ctx *TableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitOrCondition(ctx *OrConditionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitAndCondition(ctx *AndConditionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitParenthesizedCondition(ctx *ParenthesizedConditionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitNotCondition(ctx *NotConditionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitComparisonCondition(ctx *ComparisonConditionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitIntLiteral(ctx *IntLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitFloatLiteral(ctx *FloatLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitBoolLiteral(ctx *BoolLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlVisitor) VisitFunctionExpr(ctx *FunctionExprContext) interface{} {
	return v.VisitChildren(ctx)
}

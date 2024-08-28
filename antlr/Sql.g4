grammar Sql;

// Define the main grammar rules
query:   selectStatement EOF;

selectStatement: selectClause fromClause whereClause?;

selectClause: SELECT columnList;

fromClause: FROM tableName;

whereClause: WHERE conditionExpr;

columnList: columnExpr (',' columnExpr)*;

columnExpr: columnName
          | stringLiteral
          | intLiteral
          | floatLiteral
          | boolLiteral
          | functionExpr
          ;

columnName: IDENTIFIER | QUOTED_IDENTIFIER;

tableName: IDENTIFIER | QUOTED_IDENTIFIER;


// Define the different types of expressions that can appear in the WHERE clause
conditionExpr: conditionExpr AND conditionExpr          #andCondition
             | conditionExpr OR conditionExpr           #orCondition
             | NOT conditionExpr                        #notCondition
             | columnExpr comparisonOperator columnExpr #comparisonCondition
             | '(' conditionExpr ')'                    #parenthesizedCondition
             ;

// Define comparison operators
comparisonOperator: '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=';

// Define keywords
SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];  // Case-insensitive SELECT
FROM: [Ff][Rr][Oo][Mm];            // Case-insensitive FROM
WHERE: [Ww][Hh][Ee][Rr][Ee];        // Case-insensitive WHERE
AND: [Aa][Nn][Dd];                  // Case-insensitive AND
OR: [Oo][Rr];                       // Case-insensitive OR
NOT: [Nn][Oo][Tt];                  // Case-insensitive NOT

// Define token types for literals
stringLiteral: STRING;
intLiteral: INT;
floatLiteral: FLOAT;
boolLiteral: BOOL;

// Define string literal rule (SQL standard single quotes)
STRING: '\'' (ESC | ~('\\'|'\''))* '\'';

// Define the escape sequence for string literals
fragment ESC: '\\' [btnfr"'\\];

// Define integer literal rule
INT: [0-9]+;

// Define floating-point literal rule
FLOAT: [0-9]* '.' [0-9]+;

// Define boolean literal rule (simple true/false values)
BOOL: 'TRUE' | 'FALSE';

// Define function expressions
functionExpr: IDENTIFIER '(' (columnExpr (',' columnExpr)*)? ')';

// Define the IDENTIFIER token
IDENTIFIER: [a-zA-Z_][a-zA-Z_0-9]*;

// Define the QUOTED_IDENTIFIER token (for SQL standard double quotes)
QUOTED_IDENTIFIER: '"' [a-zA-Z_][a-zA-Z_0-9 ]* '"';

// Skip whitespace
WS: [ \t\r\n]+ -> skip;

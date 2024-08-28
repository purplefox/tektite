// Code generated from Sql.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"sync"
	"unicode"
)

// Suppress unused import error
var _ = fmt.Printf
var _ = sync.Once{}
var _ = unicode.IsLetter

type SqlLexer struct {
	*antlr.BaseLexer
	channelNames []string
	modeNames    []string
	// TODO: EOF string
}

var SqlLexerLexerStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	ChannelNames           []string
	ModeNames              []string
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func sqllexerLexerInit() {
	staticData := &SqlLexerLexerStaticData
	staticData.ChannelNames = []string{
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN",
	}
	staticData.ModeNames = []string{
		"DEFAULT_MODE",
	}
	staticData.LiteralNames = []string{
		"", "','", "'('", "')'", "'='", "'!='", "'<>'", "'<'", "'<='", "'>'",
		"'>='",
	}
	staticData.SymbolicNames = []string{
		"", "", "", "", "", "", "", "", "", "", "", "SELECT", "FROM", "WHERE",
		"AND", "OR", "NOT", "STRING", "INT", "FLOAT", "BOOL", "IDENTIFIER",
		"QUOTED_IDENTIFIER", "WS",
	}
	staticData.RuleNames = []string{
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8",
		"T__9", "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "STRING", "ESC",
		"INT", "FLOAT", "BOOL", "IDENTIFIER", "QUOTED_IDENTIFIER", "WS",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 0, 23, 167, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2,
		4, 7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2,
		10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15,
		7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 7,
		20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 1, 0, 1, 0, 1, 1, 1, 1, 1,
		2, 1, 2, 1, 3, 1, 3, 1, 4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 5, 1, 6, 1, 6, 1,
		7, 1, 7, 1, 7, 1, 8, 1, 8, 1, 9, 1, 9, 1, 9, 1, 10, 1, 10, 1, 10, 1, 10,
		1, 10, 1, 10, 1, 10, 1, 11, 1, 11, 1, 11, 1, 11, 1, 11, 1, 12, 1, 12, 1,
		12, 1, 12, 1, 12, 1, 12, 1, 13, 1, 13, 1, 13, 1, 13, 1, 14, 1, 14, 1, 14,
		1, 15, 1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 5, 16, 106, 8, 16, 10,
		16, 12, 16, 109, 9, 16, 1, 16, 1, 16, 1, 17, 1, 17, 1, 17, 1, 18, 4, 18,
		117, 8, 18, 11, 18, 12, 18, 118, 1, 19, 5, 19, 122, 8, 19, 10, 19, 12,
		19, 125, 9, 19, 1, 19, 1, 19, 4, 19, 129, 8, 19, 11, 19, 12, 19, 130, 1,
		20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 3, 20, 142,
		8, 20, 1, 21, 1, 21, 5, 21, 146, 8, 21, 10, 21, 12, 21, 149, 9, 21, 1,
		22, 1, 22, 1, 22, 5, 22, 154, 8, 22, 10, 22, 12, 22, 157, 9, 22, 1, 22,
		1, 22, 1, 23, 4, 23, 162, 8, 23, 11, 23, 12, 23, 163, 1, 23, 1, 23, 0,
		0, 24, 1, 1, 3, 2, 5, 3, 7, 4, 9, 5, 11, 6, 13, 7, 15, 8, 17, 9, 19, 10,
		21, 11, 23, 12, 25, 13, 27, 14, 29, 15, 31, 16, 33, 17, 35, 0, 37, 18,
		39, 19, 41, 20, 43, 21, 45, 22, 47, 23, 1, 0, 21, 2, 0, 83, 83, 115, 115,
		2, 0, 69, 69, 101, 101, 2, 0, 76, 76, 108, 108, 2, 0, 67, 67, 99, 99, 2,
		0, 84, 84, 116, 116, 2, 0, 70, 70, 102, 102, 2, 0, 82, 82, 114, 114, 2,
		0, 79, 79, 111, 111, 2, 0, 77, 77, 109, 109, 2, 0, 87, 87, 119, 119, 2,
		0, 72, 72, 104, 104, 2, 0, 65, 65, 97, 97, 2, 0, 78, 78, 110, 110, 2, 0,
		68, 68, 100, 100, 2, 0, 39, 39, 92, 92, 8, 0, 34, 34, 39, 39, 92, 92, 98,
		98, 102, 102, 110, 110, 114, 114, 116, 116, 1, 0, 48, 57, 3, 0, 65, 90,
		95, 95, 97, 122, 4, 0, 48, 57, 65, 90, 95, 95, 97, 122, 5, 0, 32, 32, 48,
		57, 65, 90, 95, 95, 97, 122, 3, 0, 9, 10, 13, 13, 32, 32, 174, 0, 1, 1,
		0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 9, 1,
		0, 0, 0, 0, 11, 1, 0, 0, 0, 0, 13, 1, 0, 0, 0, 0, 15, 1, 0, 0, 0, 0, 17,
		1, 0, 0, 0, 0, 19, 1, 0, 0, 0, 0, 21, 1, 0, 0, 0, 0, 23, 1, 0, 0, 0, 0,
		25, 1, 0, 0, 0, 0, 27, 1, 0, 0, 0, 0, 29, 1, 0, 0, 0, 0, 31, 1, 0, 0, 0,
		0, 33, 1, 0, 0, 0, 0, 37, 1, 0, 0, 0, 0, 39, 1, 0, 0, 0, 0, 41, 1, 0, 0,
		0, 0, 43, 1, 0, 0, 0, 0, 45, 1, 0, 0, 0, 0, 47, 1, 0, 0, 0, 1, 49, 1, 0,
		0, 0, 3, 51, 1, 0, 0, 0, 5, 53, 1, 0, 0, 0, 7, 55, 1, 0, 0, 0, 9, 57, 1,
		0, 0, 0, 11, 60, 1, 0, 0, 0, 13, 63, 1, 0, 0, 0, 15, 65, 1, 0, 0, 0, 17,
		68, 1, 0, 0, 0, 19, 70, 1, 0, 0, 0, 21, 73, 1, 0, 0, 0, 23, 80, 1, 0, 0,
		0, 25, 85, 1, 0, 0, 0, 27, 91, 1, 0, 0, 0, 29, 95, 1, 0, 0, 0, 31, 98,
		1, 0, 0, 0, 33, 102, 1, 0, 0, 0, 35, 112, 1, 0, 0, 0, 37, 116, 1, 0, 0,
		0, 39, 123, 1, 0, 0, 0, 41, 141, 1, 0, 0, 0, 43, 143, 1, 0, 0, 0, 45, 150,
		1, 0, 0, 0, 47, 161, 1, 0, 0, 0, 49, 50, 5, 44, 0, 0, 50, 2, 1, 0, 0, 0,
		51, 52, 5, 40, 0, 0, 52, 4, 1, 0, 0, 0, 53, 54, 5, 41, 0, 0, 54, 6, 1,
		0, 0, 0, 55, 56, 5, 61, 0, 0, 56, 8, 1, 0, 0, 0, 57, 58, 5, 33, 0, 0, 58,
		59, 5, 61, 0, 0, 59, 10, 1, 0, 0, 0, 60, 61, 5, 60, 0, 0, 61, 62, 5, 62,
		0, 0, 62, 12, 1, 0, 0, 0, 63, 64, 5, 60, 0, 0, 64, 14, 1, 0, 0, 0, 65,
		66, 5, 60, 0, 0, 66, 67, 5, 61, 0, 0, 67, 16, 1, 0, 0, 0, 68, 69, 5, 62,
		0, 0, 69, 18, 1, 0, 0, 0, 70, 71, 5, 62, 0, 0, 71, 72, 5, 61, 0, 0, 72,
		20, 1, 0, 0, 0, 73, 74, 7, 0, 0, 0, 74, 75, 7, 1, 0, 0, 75, 76, 7, 2, 0,
		0, 76, 77, 7, 1, 0, 0, 77, 78, 7, 3, 0, 0, 78, 79, 7, 4, 0, 0, 79, 22,
		1, 0, 0, 0, 80, 81, 7, 5, 0, 0, 81, 82, 7, 6, 0, 0, 82, 83, 7, 7, 0, 0,
		83, 84, 7, 8, 0, 0, 84, 24, 1, 0, 0, 0, 85, 86, 7, 9, 0, 0, 86, 87, 7,
		10, 0, 0, 87, 88, 7, 1, 0, 0, 88, 89, 7, 6, 0, 0, 89, 90, 7, 1, 0, 0, 90,
		26, 1, 0, 0, 0, 91, 92, 7, 11, 0, 0, 92, 93, 7, 12, 0, 0, 93, 94, 7, 13,
		0, 0, 94, 28, 1, 0, 0, 0, 95, 96, 7, 7, 0, 0, 96, 97, 7, 6, 0, 0, 97, 30,
		1, 0, 0, 0, 98, 99, 7, 12, 0, 0, 99, 100, 7, 7, 0, 0, 100, 101, 7, 4, 0,
		0, 101, 32, 1, 0, 0, 0, 102, 107, 5, 39, 0, 0, 103, 106, 3, 35, 17, 0,
		104, 106, 8, 14, 0, 0, 105, 103, 1, 0, 0, 0, 105, 104, 1, 0, 0, 0, 106,
		109, 1, 0, 0, 0, 107, 105, 1, 0, 0, 0, 107, 108, 1, 0, 0, 0, 108, 110,
		1, 0, 0, 0, 109, 107, 1, 0, 0, 0, 110, 111, 5, 39, 0, 0, 111, 34, 1, 0,
		0, 0, 112, 113, 5, 92, 0, 0, 113, 114, 7, 15, 0, 0, 114, 36, 1, 0, 0, 0,
		115, 117, 7, 16, 0, 0, 116, 115, 1, 0, 0, 0, 117, 118, 1, 0, 0, 0, 118,
		116, 1, 0, 0, 0, 118, 119, 1, 0, 0, 0, 119, 38, 1, 0, 0, 0, 120, 122, 7,
		16, 0, 0, 121, 120, 1, 0, 0, 0, 122, 125, 1, 0, 0, 0, 123, 121, 1, 0, 0,
		0, 123, 124, 1, 0, 0, 0, 124, 126, 1, 0, 0, 0, 125, 123, 1, 0, 0, 0, 126,
		128, 5, 46, 0, 0, 127, 129, 7, 16, 0, 0, 128, 127, 1, 0, 0, 0, 129, 130,
		1, 0, 0, 0, 130, 128, 1, 0, 0, 0, 130, 131, 1, 0, 0, 0, 131, 40, 1, 0,
		0, 0, 132, 133, 5, 84, 0, 0, 133, 134, 5, 82, 0, 0, 134, 135, 5, 85, 0,
		0, 135, 142, 5, 69, 0, 0, 136, 137, 5, 70, 0, 0, 137, 138, 5, 65, 0, 0,
		138, 139, 5, 76, 0, 0, 139, 140, 5, 83, 0, 0, 140, 142, 5, 69, 0, 0, 141,
		132, 1, 0, 0, 0, 141, 136, 1, 0, 0, 0, 142, 42, 1, 0, 0, 0, 143, 147, 7,
		17, 0, 0, 144, 146, 7, 18, 0, 0, 145, 144, 1, 0, 0, 0, 146, 149, 1, 0,
		0, 0, 147, 145, 1, 0, 0, 0, 147, 148, 1, 0, 0, 0, 148, 44, 1, 0, 0, 0,
		149, 147, 1, 0, 0, 0, 150, 151, 5, 34, 0, 0, 151, 155, 7, 17, 0, 0, 152,
		154, 7, 19, 0, 0, 153, 152, 1, 0, 0, 0, 154, 157, 1, 0, 0, 0, 155, 153,
		1, 0, 0, 0, 155, 156, 1, 0, 0, 0, 156, 158, 1, 0, 0, 0, 157, 155, 1, 0,
		0, 0, 158, 159, 5, 34, 0, 0, 159, 46, 1, 0, 0, 0, 160, 162, 7, 20, 0, 0,
		161, 160, 1, 0, 0, 0, 162, 163, 1, 0, 0, 0, 163, 161, 1, 0, 0, 0, 163,
		164, 1, 0, 0, 0, 164, 165, 1, 0, 0, 0, 165, 166, 6, 23, 0, 0, 166, 48,
		1, 0, 0, 0, 10, 0, 105, 107, 118, 123, 130, 141, 147, 155, 163, 1, 6, 0,
		0,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// SqlLexerInit initializes any static state used to implement SqlLexer. By default the
// static state used to implement the lexer is lazily initialized during the first call to
// NewSqlLexer(). You can call this function if you wish to initialize the static state ahead
// of time.
func SqlLexerInit() {
	staticData := &SqlLexerLexerStaticData
	staticData.once.Do(sqllexerLexerInit)
}

// NewSqlLexer produces a new lexer instance for the optional input antlr.CharStream.
func NewSqlLexer(input antlr.CharStream) *SqlLexer {
	SqlLexerInit()
	l := new(SqlLexer)
	l.BaseLexer = antlr.NewBaseLexer(input)
	staticData := &SqlLexerLexerStaticData
	l.Interpreter = antlr.NewLexerATNSimulator(l, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	l.channelNames = staticData.ChannelNames
	l.modeNames = staticData.ModeNames
	l.RuleNames = staticData.RuleNames
	l.LiteralNames = staticData.LiteralNames
	l.SymbolicNames = staticData.SymbolicNames
	l.GrammarFileName = "Sql.g4"
	// TODO: l.EOF = antlr.TokenEOF

	return l
}

// SqlLexer tokens.
const (
	SqlLexerT__0              = 1
	SqlLexerT__1              = 2
	SqlLexerT__2              = 3
	SqlLexerT__3              = 4
	SqlLexerT__4              = 5
	SqlLexerT__5              = 6
	SqlLexerT__6              = 7
	SqlLexerT__7              = 8
	SqlLexerT__8              = 9
	SqlLexerT__9              = 10
	SqlLexerSELECT            = 11
	SqlLexerFROM              = 12
	SqlLexerWHERE             = 13
	SqlLexerAND               = 14
	SqlLexerOR                = 15
	SqlLexerNOT               = 16
	SqlLexerSTRING            = 17
	SqlLexerINT               = 18
	SqlLexerFLOAT             = 19
	SqlLexerBOOL              = 20
	SqlLexerIDENTIFIER        = 21
	SqlLexerQUOTED_IDENTIFIER = 22
	SqlLexerWS                = 23
)

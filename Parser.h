// File: sql/Parser.hpp
#pragma once

#include "Lexer.h"
#include <string>
#include <vector>
#include <optional>
#include <variant>

namespace sql {

    /// A simple binary expression: lhs op rhs
    struct Expr {
        std::string lhs;
        std::string op;
        std::string rhs;
    };

    /// AST for each statement type
    struct SelectStmt {
        std::vector<std::string> columns;
        std::string              table;
        std::optional<Expr>      where;
    };

    struct InsertStmt {
        std::string              table;
        std::vector<std::string> columns;  // may be empty => all columns
        std::vector<std::string> values;
    };

    struct UpdateStmt {
        std::string                             table;
        std::vector<std::pair<std::string, std::string>> assignments;
        std::optional<Expr>                     where;
    };

    struct DeleteStmt {
        std::string         table;
        std::optional<Expr> where;
    };

    struct TransactionStmt {
        bool isBegin;  // true => BEGIN, false => COMMIT/ROLLBACK
    };

    using Statement = std::variant<
        SelectStmt,
        InsertStmt,
        UpdateStmt,
        DeleteStmt,
        TransactionStmt
    >;

    /// Parser: consumes tokens from Lexer and produces a Statement
    class Parser {
    public:
        explicit Parser(Lexer& lexer);

        /// Parse exactly one statement (must end in SEMICOLON or END)
        Statement parseStatement();

    private:
        Lexer& _lex;
        Token  _cur;

        /// Advance to next token, returning it
        Token nextToken();

        /// Consume token if it matches `t`
        bool accept(TokenType t);

        /// Expect token `t`, or throw a runtime_error
        void expect(TokenType t);

        // Per‑statement parse routines
        SelectStmt      parseSelect();
        InsertStmt      parseInsert();
        UpdateStmt      parseUpdate();
        DeleteStmt      parseDelete();
        TransactionStmt parseTransaction();

        // Helpers
        std::vector<std::string> parseIdentifierList();
        Expr parseExpr();
    };

} // namespace sql

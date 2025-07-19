// File: sql/Parser.hpp
#pragma once

#include "Lexer.h"
#include "AST.h"          // defines ASTNode, SelectNode, InsertNode, etc.
#include <memory>
#include <string>
#include <vector>
#include <optional>

namespace sql {

    class Parser {
    public:
        explicit Parser(Lexer& lexer);

        /// Parse exactly one statement (ending in SEMICOLON) and return its AST.
        /// Throws std::runtime_error on any syntax error.
        std::unique_ptr<ASTNode> parseStatement();

    private:
        Lexer& _lex;
        Token  _cur;

        /// Consume and return the next token.
        Token nextToken();

        /// If next token is `t`, consume and return true; else false.
        bool accept(TokenType t);

        /// If next token is not `t`, throw; else consume it.
        void expect(TokenType t);

        // For each SQL type, build the proper ASTNode subclass:
        std::unique_ptr<SelectNode>     parseSelect();
        std::unique_ptr<InsertNode>     parseInsert();
        std::unique_ptr<UpdateNode>     parseUpdate();
        std::unique_ptr<DeleteNode>     parseDelete();
        std::unique_ptr<TransactionNode> parseTransaction();
        std::unique_ptr<CreateNode> parseCreate();

        // Helpers:
        /// Parse a comma‐separated list of identifiers.
        std::vector<std::string> parseIdentifierList();

        /// Parse a simple expression of form `lhs op rhs`
        Expression parseExpression();
    };

} // namespace sql

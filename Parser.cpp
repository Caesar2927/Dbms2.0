// File: sql/Parser.cpp
#include "Parser.h"
#include <stdexcept>
#include <cctype>

namespace sql {

    Parser::Parser(Lexer& lex)
        : _lex(lex), _cur(_lex.nextToken())
    {
    }

    /// Advance to next token
    Token Parser::nextToken() {
        _cur = _lex.nextToken();
        return _cur;
    }

    /// If current token matches `t`, consume and return true
    bool Parser::accept(TokenType t) {
        if (_cur.type == t) {
            nextToken();
            return true;
        }
        return false;
    }

    /// Require current token to be `t`, else error
    void Parser::expect(TokenType t) {
        if (_cur.type != t) {
            throw std::runtime_error("Parser: expected token " +
                std::to_string(int(t)) + " at pos " +
                std::to_string(_cur.position));
        }
        nextToken();
    }

    /// Parse a full statement
    Statement Parser::parseStatement() {
        Statement stmt;
        switch (_cur.type) {
        case TokenType::SELECT:
            stmt = parseSelect();
            break;
        case TokenType::INSERT:
            stmt = parseInsert();
            break;
        case TokenType::UPDATE:
            stmt = parseUpdate();
            break;
        case TokenType::DELETE_:
            stmt = parseDelete();
            break;
        case TokenType::BEGIN:
        case TokenType::COMMIT:
        case TokenType::ROLLBACK:
            stmt = parseTransaction();
            break;
        default:
            throw std::runtime_error("Unsupported statement at pos " +
                std::to_string(_cur.position));
        }
        // Statements must end with semicolon or EOF
        if (_cur.type == TokenType::SEMICOLON) nextToken();
        return stmt;
    }

    /// SELECT col1, col2 FROM table [ WHERE expr ]
    SelectStmt Parser::parseSelect() {
        nextToken();  // consume SELECT
        SelectStmt s;
        // columns
        if (accept(TokenType::ASTERISK)) {
            s.columns.push_back("*");
        }
        else {
            s.columns = parseIdentifierList();
        }
        expect(TokenType::FROM);
        if (_cur.type != TokenType::IDENTIFIER)
            throw std::runtime_error("Expected table name");
        s.table = _cur.text;
        nextToken();

        // optional WHERE
        if (accept(TokenType::WHERE)) {
            s.where = parseExpr();
        }
        return s;
    }

    /// INSERT INTO table [(col1,col2)] VALUES (v1,v2)
    InsertStmt Parser::parseInsert() {
        nextToken();            // INSERT
        expect(TokenType::INTO);
        InsertStmt ins;
        if (_cur.type != TokenType::IDENTIFIER)
            throw std::runtime_error("Expected table name");
        ins.table = _cur.text;
        nextToken();

        if (accept(TokenType::LPAREN)) {
            ins.columns = parseIdentifierList();
            expect(TokenType::RPAREN);
        }
        expect(TokenType::VALUES);
        expect(TokenType::LPAREN);
        // values are literals or numbers or identifiers
        while (true) {
            if (_cur.type != TokenType::STRING_LITERAL &&
                _cur.type != TokenType::NUMERIC_LITERAL &&
                _cur.type != TokenType::IDENTIFIER) {
                throw std::runtime_error("Expected literal or identifier in VALUES");
            }
            ins.values.push_back(_cur.text);
            nextToken();
            if (accept(TokenType::COMMA)) continue;
            break;
        }
        expect(TokenType::RPAREN);
        return ins;
    }

    /// UPDATE table SET col=val, ... [ WHERE expr ]
    UpdateStmt Parser::parseUpdate() {
        nextToken();  // UPDATE
        UpdateStmt up;
        if (_cur.type != TokenType::IDENTIFIER)
            throw std::runtime_error("Expected table name in UPDATE");
        up.table = _cur.text;
        nextToken();
        expect(TokenType::SET);
        while (true) {
            if (_cur.type != TokenType::IDENTIFIER)
                throw std::runtime_error("Expected column name in SET");
            std::string col = _cur.text;
            nextToken();
            expect(TokenType::EQ);
            if (_cur.type != TokenType::STRING_LITERAL &&
                _cur.type != TokenType::NUMERIC_LITERAL &&
                _cur.type != TokenType::IDENTIFIER) {
                throw std::runtime_error("Expected value after = in SET");
            }
            std::string val = _cur.text;
            nextToken();
            up.assignments.emplace_back(col, val);
            if (accept(TokenType::COMMA)) continue;
            break;
        }
        if (accept(TokenType::WHERE)) {
            up.where = parseExpr();
        }
        return up;
    }

    /// DELETE FROM table [ WHERE expr ]
    DeleteStmt Parser::parseDelete() {
        nextToken();  // DELETE
        expect(TokenType::FROM);
        DeleteStmt d;
        if (_cur.type != TokenType::IDENTIFIER)
            throw std::runtime_error("Expected table name in DELETE");
        d.table = _cur.text;
        nextToken();
        if (accept(TokenType::WHERE)) {
            d.where = parseExpr();
        }
        return d;
    }

    /// BEGIN | COMMIT | ROLLBACK
    TransactionStmt Parser::parseTransaction() {
        TransactionStmt t;
        if (_cur.type == TokenType::BEGIN) {
            t.isBegin = true;
        }
        else {
            t.isBegin = false;  // either COMMIT or ROLLBACK
        }
        nextToken();
        return t;
    }

    /// col op val
    Expr Parser::parseExpr() {
        Expr e;
        if (_cur.type != TokenType::IDENTIFIER)
            throw std::runtime_error("Expected identifier in expression");
        e.lhs = _cur.text;  nextToken();

        // operator
        switch (_cur.type) {
        case TokenType::EQ:   e.op = "="; break;
        case TokenType::NEQ:  e.op = "!="; break;
        case TokenType::LT:   e.op = "<"; break;
        case TokenType::LTE:  e.op = "<="; break;
        case TokenType::GT:   e.op = ">"; break;
        case TokenType::GTE:  e.op = ">="; break;
        default:
            throw std::runtime_error("Expected comparison operator");
        }
        e.op = _cur.text;
        nextToken();

        if (_cur.type != TokenType::STRING_LITERAL &&
            _cur.type != TokenType::NUMERIC_LITERAL &&
            _cur.type != TokenType::IDENTIFIER)
        {
            throw std::runtime_error("Expected literal or identifier on rhs");
        }
        e.rhs = _cur.text;
        nextToken();
        return e;
    }

    /// Parses a comma?separated list of identifiers (no surrounding parens)
    std::vector<std::string> Parser::parseIdentifierList() {
        std::vector<std::string> cols;
        while (true) {
            if (_cur.type != TokenType::IDENTIFIER) {
                throw std::runtime_error("Expected identifier in list");
            }
            cols.push_back(_cur.text);
            nextToken();
            if (!accept(TokenType::COMMA)) break;
        }
        return cols;
    }

} // namespace sql

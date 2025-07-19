// File: sql/Parser.cpp

#include "Parser.h"
#include <stdexcept>
#include <cctype>
using namespace sql;

Parser::Parser(Lexer& lex)
    : _lex(lex)
{
    // Prime the first token
    _cur = _lex.nextToken();
}

Token Parser::nextToken() {
    return _cur = _lex.nextToken();
}

bool Parser::accept(TokenType t) {
    if (_cur.type == t) {
        nextToken();
        return true;
    }
    return false;
}

void Parser::expect(TokenType t) {
    if (_cur.type != t) {
        throw std::runtime_error(
            "Parser error: expected token " + std::to_string(int(t)) +
            " at pos " + std::to_string(_cur.position));
    }
    nextToken();
}

std::unique_ptr<ASTNode> Parser::parseStatement() {
    switch (_cur.type) {
    case TokenType::SELECT:  return parseSelect();
    case TokenType::INSERT:  return parseInsert();
    case TokenType::UPDATE:  return parseUpdate();
    case TokenType::DELETE_: return parseDelete();
    case TokenType::BEGIN:
    case TokenType::COMMIT:
    case TokenType::ROLLBACK:
    case TokenType::CREATE:   return parseCreate();
        return parseTransaction();
    default:
        throw std::runtime_error(
            "Parser error: unexpected token at pos " +
            std::to_string(_cur.position));
    }
}

std::unique_ptr<SelectNode> Parser::parseSelect() {
    auto node = std::make_unique<SelectNode>();
    expect(TokenType::SELECT);

    // column list
    if (accept(TokenType::ASTERISK)) {
        node->columns.push_back("*");
    }
    else {
        node->columns = parseIdentifierList();
    }

    expect(TokenType::FROM);
    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected table name at pos "
            + std::to_string(_cur.position));
    }
    node->table = _cur.text;
    nextToken();

    if (accept(TokenType::WHERE)) {
        node->whereClause = parseExpression();
    }

    expect(TokenType::SEMICOLON);
    return node;
}

std::unique_ptr<InsertNode> Parser::parseInsert() {
    auto node = std::make_unique<InsertNode>();
    expect(TokenType::INSERT);
    expect(TokenType::INTO);

    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected table name at pos "
            + std::to_string(_cur.position));
    }
    node->table = _cur.text;
    nextToken();

    // optional column list
    if (accept(TokenType::LPAREN)) {
        node->columns = parseIdentifierList();
        expect(TokenType::RPAREN);
    }

    expect(TokenType::VALUES);
    expect(TokenType::LPAREN);

    // values list
    while (true) {
        if (_cur.type != TokenType::STRING_LITERAL
            && _cur.type != TokenType::NUMERIC_LITERAL)
        {
            throw std::runtime_error("Parser error: expected literal at pos "
                + std::to_string(_cur.position));
        }
        node->values.push_back(_cur.text);
        nextToken();
        if (accept(TokenType::COMMA)) continue;
        break;
    }

    expect(TokenType::RPAREN);
    expect(TokenType::SEMICOLON);
    return node;
}


std::unique_ptr<CreateNode> Parser::parseCreate() {
    auto node = std::make_unique<CreateNode>();
    expect(TokenType::CREATE);
    expect(TokenType::TABLE);

    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected table name at pos " +
            std::to_string(_cur.position));
    }
    node->table = _cur.text;
    nextToken();

    expect(TokenType::LPAREN);

    while (_cur.type == TokenType::IDENTIFIER) {
        std::string colName = _cur.text;
        nextToken();

        if (_cur.type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Parser error: expected column type at pos " +
                std::to_string(_cur.position));
        }
        std::string colType = _cur.text;
        nextToken();

        node->columns.emplace_back(colName, colType);

        if (!accept(TokenType::COMMA)) break;
    }

    // Handle optional PRIMARY KEY(...)
    if (_cur.type == TokenType::PRIMARY) {
        nextToken();
        expect(TokenType::KEY);
        expect(TokenType::LPAREN);
        node->primaryKeys = parseIdentifierList();
        expect(TokenType::RPAREN);
    }

    expect(TokenType::RPAREN);
    expect(TokenType::SEMICOLON);

    return node;
}




std::unique_ptr<UpdateNode> Parser::parseUpdate() {
    auto node = std::make_unique<UpdateNode>();
    expect(TokenType::UPDATE);

    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected table name at pos "
            + std::to_string(_cur.position));
    }
    node->table = _cur.text;
    nextToken();

    expect(TokenType::SET);
    // assignments: col = literal {, col = literal, ...}
    while (true) {
        if (_cur.type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Parser error: expected column at pos "
                + std::to_string(_cur.position));
        }
        std::string col = _cur.text;
        nextToken();
        expect(TokenType::EQ);
        if (_cur.type != TokenType::STRING_LITERAL
            && _cur.type != TokenType::NUMERIC_LITERAL)
        {
            throw std::runtime_error("Parser error: expected literal at pos "
                + std::to_string(_cur.position));
        }
        std::string val = _cur.text;
        nextToken();

        node->assignments.emplace_back(col, val);
        if (accept(TokenType::COMMA)) continue;
        break;
    }

    if (accept(TokenType::WHERE)) {
        node->whereClause = parseExpression();  
    }

    expect(TokenType::SEMICOLON);
    return node;
}

std::unique_ptr<DeleteNode> Parser::parseDelete() {
    auto node = std::make_unique<DeleteNode>();
    expect(TokenType::DELETE_);
    expect(TokenType::FROM);

    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected table name at pos "
            + std::to_string(_cur.position));
    }
    node->table = _cur.text;
    nextToken();

    if (accept(TokenType::WHERE)) {
        node->whereClause = parseExpression();
    }

    expect(TokenType::SEMICOLON);
    return node;
}

std::unique_ptr<TransactionNode> Parser::parseTransaction() {
    TransactionNode::Action act;
    if (accept(TokenType::BEGIN)) {
        act = TransactionNode::Action::Begin;
    }
    else if (accept(TokenType::COMMIT)) {
        act = TransactionNode::Action::Commit;
    }
    else if (accept(TokenType::ROLLBACK)) {
        act = TransactionNode::Action::Rollback;
    }
    else {
        throw std::runtime_error("Parser error: unexpected transaction keyword at pos "
            + std::to_string(_cur.position));
    }
    expect(TokenType::SEMICOLON);
    return std::make_unique<TransactionNode>(act);
}







std::vector<std::string> Parser::parseIdentifierList() {
    std::vector<std::string> ids;
    while (true) {
        if (_cur.type != TokenType::IDENTIFIER) {
            throw std::runtime_error("Parser error: expected identifier at pos "
                + std::to_string(_cur.position));
        }
        ids.push_back(_cur.text);
        nextToken();
        if (!accept(TokenType::COMMA)) break;
    }
    return ids;
}

Expression Parser::parseExpression() {
    // very simple: lhs op rhs  (no parentheses, no AND/OR)
    Expression e;
    if (_cur.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Parser error: expected identifier in expression at pos "
            + std::to_string(_cur.position));
    }
    e.lhs = _cur.text;
    nextToken();

    // operator
    switch (_cur.type) {
    case TokenType::EQ:  e.op = "=";  break;
    case TokenType::NEQ: e.op = "!="; break;
    case TokenType::LT:  e.op = "<";  break;
    case TokenType::LTE: e.op = "<="; break;
    case TokenType::GT:  e.op = ">";  break;
    case TokenType::GTE: e.op = ">="; break;
    default:
        throw std::runtime_error("Parser error: expected comparison operator at pos "
            + std::to_string(_cur.position));
    }
    nextToken();

    // rhs literal or identifier
    if (_cur.type != TokenType::STRING_LITERAL
        && _cur.type != TokenType::NUMERIC_LITERAL
        && _cur.type != TokenType::IDENTIFIER)
    {
        throw std::runtime_error("Parser error: expected literal or identifier at pos "
            + std::to_string(_cur.position));
    }
    e.rhs = _cur.text;
    nextToken();
    return e;
}

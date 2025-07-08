// File: sql/Lexer.cpp
#include "Lexer.h"
#include <cctype>
#include <unordered_map>

namespace sql {

    static const std::unordered_map<std::string, TokenType> KEYWORDS = {
        {"SELECT", TokenType::SELECT},
        {"INSERT", TokenType::INSERT},
        {"UPDATE", TokenType::UPDATE},
        {"DELETE", TokenType::DELETE_},
        {"FROM",   TokenType::FROM},
        {"WHERE",  TokenType::WHERE},
        {"ORDER",  TokenType::ORDER},
        {"BY",     TokenType::BY},
        {"INTO",   TokenType::INTO},
        {"VALUES", TokenType::VALUES},
        {"SET",    TokenType::SET},
        {"BEGIN",  TokenType::BEGIN},
        {"COMMIT", TokenType::COMMIT},
        {"ROLLBACK", TokenType::ROLLBACK},
        { "JOIN", TokenType::JOIN },
        {"INNERJOIN", TokenType::INNERJOIN},
        {"LEFTJOIN", TokenType::LEFTJOIN},
        {"RIGHTJOIN", TokenType::RIGHTJOIN},
        {"FULLJOIN", TokenType::FULLJOIN},
        {"OUTERJOIN", TokenType::OUTERJOIN},
        {"ON", TokenType::ON}
    };

    Lexer::Lexer(const std::string& input)
        : _input(input), _pos(0) {
    }

    Token Lexer::peekToken() {
        if (!_hasPeek) {
            _peeked = nextToken();
            _hasPeek = true;
        }
        return _peeked;
    }

    Token Lexer::nextToken() {
        if (_hasPeek) {
            _hasPeek = false;
            return _peeked;
        }
        skipWhitespaceAndComments();
        size_t start = _pos;
        if (_pos >= _input.size()) {
            return makeToken(TokenType::END, _pos, 0);
        }
        char c = peekChar();
        if (std::isalpha(c) || c == '_') {
            return lexIdentifierOrKeyword();
        }
        if (std::isdigit(c)) {
            return lexNumber();
        }
        if (c == '\'') {
            return lexString();
        }
        // operators and punctuation
        return lexOperatorOrPunct();
    }

    void Lexer::skipWhitespaceAndComments() {
        while (_pos < _input.size()) {
            char c = peekChar();
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                _pos++;
            }
            else if (c == '-' && _pos + 1 < _input.size() && _input[_pos + 1] == '-') {
                // skip to end of line
                _pos += 2;
                while (_pos < _input.size() && peekChar() != '\n') _pos++;
            }
            else {
                break;
            }
        }
    }

    char Lexer::peekChar() const {
        return _input[_pos];
    }

    char Lexer::getChar() {
        return _input[_pos++];
    }

    bool Lexer::match(char expected) {
        if (_pos < _input.size() && _input[_pos] == expected) {
            _pos++;
            return true;
        }
        return false;
    }

    Token Lexer::makeToken(TokenType type, size_t start, size_t len) {
        return Token{
            type,
            _input.substr(start, len),
            static_cast<int>(start)
        };
    }

    Token Lexer::lexIdentifierOrKeyword() {
        size_t start = _pos;
        while (_pos < _input.size() &&
            (std::isalnum(peekChar()) || peekChar() == '_')) {
            _pos++;
        }
        size_t len = _pos - start;
        std::string text = _input.substr(start, len);
        // uppercase for keyword lookup
        std::string up;
        up.reserve(len);
        for (char c : text) up.push_back(std::toupper(c));
        auto it = KEYWORDS.find(up);
        if (it != KEYWORDS.end()) {
            return Token{ it->second, up, static_cast<int>(start) };
        }
        return Token{ TokenType::IDENTIFIER, text, static_cast<int>(start) };
    }

    Token Lexer::lexNumber() {
        size_t start = _pos;
        while (_pos < _input.size() && std::isdigit(peekChar()))
            _pos++;
        // optional fractional part
        if (peekChar() == '.') {
            _pos++;
            while (_pos < _input.size() && std::isdigit(peekChar()))
                _pos++;
        }
        return makeToken(TokenType::NUMERIC_LITERAL, start, _pos - start);
    }

    Token Lexer::lexString() {
        size_t start = _pos;
        getChar(); // skip opening '
        while (_pos < _input.size()) {
            char c = getChar();
            if (c == '\'') {
                if (peekChar() == '\'') {
                    // escaped single quote
                    getChar();
                }
                else {
                    break;
                }
            }
        }
        // include the surrounding quotes in text
        return makeToken(TokenType::STRING_LITERAL, start, _pos - start);
    }

    Token Lexer::lexOperatorOrPunct() {
        size_t start = _pos;
        char c = getChar();
        switch (c) {
        case '=': return makeToken(TokenType::EQ, start, 1);
        case '<':
            if (match('=')) return makeToken(TokenType::LTE, start, 2);
            else if (match('>')) return makeToken(TokenType::NEQ, start, 2);
            else return makeToken(TokenType::LT, start, 1);
        case '>':
            if (match('=')) return makeToken(TokenType::GTE, start, 2);
            else return makeToken(TokenType::GT, start, 1);
        case '!':
            if (match('=')) return makeToken(TokenType::NEQ, start, 2);
            break;
        case ',': return makeToken(TokenType::COMMA, start, 1);
        case ';': return makeToken(TokenType::SEMICOLON, start, 1);
        case '(': return makeToken(TokenType::LPAREN, start, 1);
        case ')': return makeToken(TokenType::RPAREN, start, 1);
        case '*': return makeToken(TokenType::ASTERISK, start, 1);
        default: break;
        }
        // unrecognized
        return makeToken(TokenType::END, start, 1);
    }

} // namespace sql

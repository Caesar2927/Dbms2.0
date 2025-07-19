// File: sql/Lexer.hpp
#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace sql {

    enum class TokenType {
        // End of input
        END,

        // Keywords
        SELECT, INSERT, UPDATE, DELETE_,
        FROM, WHERE, ORDER, BY,
        INTO, VALUES, SET,
        BEGIN, COMMIT, ROLLBACK,
        // joins
        JOIN,       // simple JOIN
        INNERJOIN,
        LEFTJOIN,
        RIGHTJOIN,
        FULLJOIN,
        OUTERJOIN,
        ON,
        // Identifiers & literals
        CREATE,    
        TABLE,     
        PRIMARY,   
        KEY,       
        IDENTIFIER,
        NUMERIC_LITERAL,
        STRING_LITERAL,

        // Operators
        EQ,      // =
        NEQ,     // !=
        LT,      // <
        LTE,     // <=
        GT,      // >
        GTE,     // >=

        // Punctuation
        COMMA,   // ,
        SEMICOLON, // ;
        LPAREN,  // (
        RPAREN,  // )
        ASTERISK // *
    };

    struct Token {
        TokenType    type;
        std::string  text;  // actual text (upper?cased for keywords)
        int          position; // index in input where token starts
    };

    class Lexer {
    public:
        explicit Lexer(const std::string& input);

        /// Return the next token (consumes it).
        Token nextToken();

        /// Peek at the next token without consuming.
        Token peekToken();

    private:
        const std::string  _input;
        size_t             _pos = 0;
        Token              _peeked;
        bool               _hasPeek = false;

        void  skipWhitespaceAndComments();
        char  peekChar() const;
        char  getChar();
        bool  match(char expected);
        Token makeToken(TokenType, size_t start, size_t len);

        Token lexIdentifierOrKeyword();
        Token lexNumber();
        Token lexString();
        Token lexOperatorOrPunct();
    };

} // namespace sql

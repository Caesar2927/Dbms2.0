// File: sqlinterface.cpp
#include "sqlinterface.h"
#include <algorithm>
#include <cctype>

SQLInterface::SQLInterface()
    : _walMgr("wal.log")
    , _txnMgr(_lockMgr, _walMgr)
    , _executor(_bufMgr)
{
    // We only need this one static call to wire in the buffer for all catalog lookups:
   
}

void SQLInterface::run() {
    std::string line;
    while (true) {
        std::cout << "sql> ";
        if (!std::getline(std::cin, line)) break;

        // EXIT shortcut
        {
            auto up = line;
            std::transform(up.begin(), up.end(), up.begin(),
                [](unsigned char c) { return std::toupper(c); });
            if (up == "EXIT" || up == "EXIT;") {
                std::cout << "Goodbye.\n";
                break;
            }
        }

        try {
            sql::Lexer  lexer(line);
            sql::Parser parser(lexer);
            auto ast = parser.parseStatement();
            _executor.execute(ast);
        }
        catch (const std::exception& ex) {
            std::cerr << "[SQL Error] " << ex.what() << "\n";
        }
    }
}

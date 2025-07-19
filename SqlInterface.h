// File: sqlinterface.hpp
#pragma once

#include <string>
#include <iostream>

#include "BufferManager.h"
#include "LockManager.h"
#include "WALManager.h"
#include "TransactionManager.h"

#include "Lexer.h"
#include "Parser.h"
#include "Executor.h"
#include "CatalogManager.h"
#include "TransactionController.h"

class SQLInterface {
public:
    SQLInterface();

    /// Read lines from stdin, parse & execute.  EXIT or EXIT; quits.
    void run();

private:
    BufferManager      _bufMgr;
    LockManager        _lockMgr;
    WALManager         _walMgr;
    TransactionManager _txnMgr;
    sql::Executor      _executor;
};

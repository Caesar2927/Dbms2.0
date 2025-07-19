// File: Executor.cpp
#include "Executor.h"
#include "record_manager_sql.h"
#include "table_manager.h"
#include "Schema.h"
#include <iostream>
#include <iomanip>

using namespace sql;

Executor::Executor(BufferManager& bufMgr)
    : _bufMgr(bufMgr)
{
    // Let the catalog know how to pin pages, etc.
    CatalogManager::init(&_bufMgr);
}

void Executor::execute(const AST& ast) {
    switch (ast->nodeType()) {
    case ASTNode::NodeType::Select:
        execSelect(*static_cast<const SelectNode*>(ast.get()));
        break;
    case ASTNode::NodeType::Insert:
        execInsert(*static_cast<const InsertNode*>(ast.get()));
        break;
    case ASTNode::NodeType::Update:
        execUpdate(*static_cast<const UpdateNode*>(ast.get()));
        break;
    case ASTNode::NodeType::Delete:
        execDelete(*static_cast<const DeleteNode*>(ast.get()));
        break;
    case ASTNode::NodeType::Transaction:
        execTransaction(*static_cast<const TransactionNode*>(ast.get()));
        break;
    case ASTNode::NodeType::Create:
        execCreate(*static_cast<const CreateNode*>(ast.get()));
        break;
    }
}

void Executor::execSelect(const SelectNode& s) {
    if (!s.whereClause) {
        auto rows = RecordManagerSQL::scanAll(s.table);
        for (auto& r : rows) {
            for (auto& v : r) std::cout << v << " ";
            std::cout << "\n";
        }
        return;
    }

    auto& expr = *s.whereClause;
    if (expr.op == "=") {
        if (auto opt = RecordManagerSQL::findRecord(s.table, expr.lhs, expr.rhs)) {
            for (auto& v : *opt) std::cout << v << " ";
            std::cout << "\n";
        }
    }
    else if (expr.op == ">=") {
        auto rows = RecordManagerSQL::scanGreaterEqual(s.table, expr.lhs, expr.rhs);
        for (auto& r : rows) {
            for (auto& v : r) std::cout << v << " ";
            std::cout << "\n";
        }
    }
    else if (expr.op == "<=") {
        auto rows = RecordManagerSQL::scanLessEqual(s.table, expr.lhs, expr.rhs);
        for (auto& r : rows) {
            for (auto& v : r) std::cout << v << " ";
            std::cout << "\n";
        }
    }
    else {
        auto rows = RecordManagerSQL::scanAll(s.table);
        for (auto& r : rows) {
            for (auto& v : r) std::cout << v << " ";
            std::cout << "\n";
        }
    }
}

void Executor::execInsert(const InsertNode& ins) {
    std::cout << "[EXEC] INSERT into " << ins.table << "\n";
    long off = RecordManagerSQL::insertRecord(ins.table, ins.values);
    if (off < 0) std::cerr << "[EXEC] INSERT failed\n";
    else         std::cout << "[EXEC] INSERT at offset " << off << "\n";
}

void Executor::execUpdate(const UpdateNode& upd) {
    std::cout << "[EXEC] UPDATE not implemented yet\n";
}

void Executor::execDelete(const DeleteNode& del) {
    std::cout << "[EXEC] DELETE from " << del.table << "\n";

    if (!del.whereClause) {
        std::cerr << "[EXEC] DELETE requires WHERE <field>=<value>\n";
        return;
    }

    const auto& expr = *del.whereClause;
    if (expr.op != "=") {
        std::cerr << "[EXEC] DELETE only supports '=' predicates\n";
        return;
    }

    const std::string& fieldName = expr.lhs;
    const std::string& value = expr.rhs;

    auto res = RecordManagerSQL::deleteRecord(del.table, fieldName, value);
    if (res == DMLResult::Deleted) {
        std::cout << "[EXEC] DELETE succeeded\n";
    }
    else if (res == DMLResult::NotFound) {
        std::cout << "[EXEC] DELETE: no matching row\n";
    }
    else {
        std::cerr << "[EXEC] DELETE error\n";
    }
}

void Executor::execTransaction(const TransactionNode& t) {
    switch (t.action) {
    case TransactionNode::Action::Begin:
        std::cout << "[EXEC] BEGIN TRANSACTION\n";
        break;
    case TransactionNode::Action::Commit:
        std::cout << "[EXEC] COMMIT\n";
        break;
    case TransactionNode::Action::Rollback:
        std::cout << "[EXEC] ROLLBACK\n";
        break;
    }
}

void Executor::execCreate(const CreateNode& c) {
    std::string schemaStr;
    for (size_t i = 0; i < c.columns.size(); ++i) {
        schemaStr += c.columns[i].second + " " + c.columns[i].first;
        if (i + 1 < c.columns.size()) schemaStr += ", ";
    }

    std::string keysStr;
    for (size_t i = 0; i < c.primaryKeys.size(); ++i) {
        keysStr += c.primaryKeys[i];
        if (i + 1 < c.primaryKeys.size()) keysStr += ",";
    }

    try {
        TableManager::createTable(c.table, schemaStr, keysStr);
        std::cout << "[EXEC] Table '" << c.table << "' created.\n";
    }
    catch (const std::exception& e) {
        std::cerr << "[EXEC][ERROR] CREATE TABLE: " << e.what() << "\n";
    }
}

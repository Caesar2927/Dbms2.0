// File: Executor.h
#pragma once

#include "AST.h"
#include "CatalogManager.h"
#include "record_manager_sql.h"   // <-- your new SQL‐style interface
#include "BufferManager.h"

namespace sql {

    class Executor {
    public:
        explicit Executor(BufferManager& bufMgr);

        /// Dispatch based on the AST node type
        void execute(const AST& ast);

    private:
        BufferManager& _bufMgr;

        void execSelect(const SelectNode& s);
        void execInsert(const InsertNode& ins);
        void execUpdate(const UpdateNode& upd);
        void execDelete(const DeleteNode& del);
        void execTransaction(const TransactionNode& t);
        void execCreate(const CreateNode& c);
    };

} // namespace sql

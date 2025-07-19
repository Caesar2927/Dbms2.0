#pragma once

#include "ASTNode.h"
#include <memory>

namespace sql {

    /// A generic AST pointer type
    using AST = std::unique_ptr<ASTNode>;

    /// Helpers to create each node type more concisely:

    static inline AST makeSelectNode() {
        return std::make_unique<SelectNode>();
    }

    static inline AST makeInsertNode() {
        return std::make_unique<InsertNode>();
    }

    static inline AST makeUpdateNode() {
        return std::make_unique<UpdateNode>();
    }
   
    
    static inline AST makeCreateeNode() {
        return std::make_unique<CreateNode>();
    }

    static inline AST makeDeleteNode() {
        return std::make_unique<DeleteNode>();
    }

    static inline AST makeTransactionNode(TransactionNode::Action act) {
        return std::make_unique<TransactionNode>(act);
    }

} // namespace sql

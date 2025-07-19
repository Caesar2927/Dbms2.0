#pragma once

#include <string>
#include <vector>
#include <optional>

namespace sql {

    /// A simple binary comparison expression, e.g. “age >= 18”
    struct Expression {
        std::string lhs;   // left side, e.g. column name
        std::string op;    // operator: =, !=, <, <=, >, >=
        std::string rhs;   // right side, e.g. value or column
    };

    /// Base class for all AST nodes
    class ASTNode {
    public:
        enum class NodeType {
            Select,
            Create,
            Insert,
            Update,
            Delete,
            Transaction,
           
        };

        explicit ASTNode(NodeType t) : type(t) {}
        virtual ~ASTNode() = default;

        NodeType nodeType() const { return type; }

    private:
        NodeType type;
    };

    /// AST for: SELECT col1, col2 FROM table [ WHERE expr ]
    class SelectNode : public ASTNode {
    public:
        SelectNode() : ASTNode(NodeType::Select) {}

        std::vector<std::string> columns;      // list of column names or {"*"}
        std::string              table;        // table name
        std::optional<Expression> whereClause; // optional WHERE
    };

    /// AST for: INSERT INTO table [(col1,...)] VALUES (v1,...)
    class InsertNode : public ASTNode {
    public:
        InsertNode() : ASTNode(NodeType::Insert) {}

        std::string              table;
        std::vector<std::string> columns;  // may be empty => all columns
        std::vector<std::string> values;   // each literal/value
    };

    /// AST for: UPDATE table SET col=val[, ...] [ WHERE expr ]
    class UpdateNode : public ASTNode {
    public:
        UpdateNode() : ASTNode(NodeType::Update) {}

        std::string                                      table;
        std::vector<std::pair<std::string, std::string>> assignments; // (col, value)
        std::optional<Expression>                        whereClause;
    };

    struct CreateNode : ASTNode {
        CreateNode() : ASTNode(NodeType::Create) {}

        std::string table;
        // list of (colName, colType)
        std::vector<std::pair<std::string, std::string>> columns;
        // optional list of PK columns
        std::vector<std::string> primaryKeys;
    };

    /// AST for: DELETE FROM table [ WHERE expr ]
    class DeleteNode : public ASTNode {
    public:
        DeleteNode() : ASTNode(NodeType::Delete) {}

        std::string              table;
        std::optional<Expression> whereClause;
    };

    /// AST for transaction control: BEGIN; COMMIT; or ROLLBACK;
    class TransactionNode : public ASTNode {
    public:
        enum class Action { Begin, Commit, Rollback };

        explicit TransactionNode(Action a)
            : ASTNode(NodeType::Transaction), action(a) {
        }

        Action action;
    };

} // namespace sql

#pragma once
#include <string>
#include <vector>

class Schema {
public:
    struct Field {
        std::string type;
        std::string name;
        int length;  // Only used for strings
    };

    Schema(const std::string& schemaStr, const std::string& uniqueKeysStr);
    void saveToFile(const std::string& path);
    std::vector<Field> getFields() const;
    std::vector<std::string> getUniqueKeys() const;

    int getRecordSize() const {
        int sum = 0;
        for (auto& f : fields) sum += f.length;
        return sum;
    }


private:
    std::vector<Field> fields;
    std::vector<std::string> uniqueKeys;
};

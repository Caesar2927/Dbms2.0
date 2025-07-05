#include "schema.h"
#include <fstream>
#include <sstream>
#include <iostream>

Schema::Schema(const std::string& schemaStr, const std::string& uniqueKeysStr) {
    std::stringstream ss(schemaStr);
    std::string token;

    while (std::getline(ss, token, ',')) {
        std::stringstream fieldStream(token);
        std::string type, name;
        fieldStream >> type >> name;

        Field f;
        f.type = type;
        f.name = name;
        f.length = (type == "string") ? 40 : sizeof(int); // default string size

        fields.push_back(f);
    }

    std::stringstream keyStream(uniqueKeysStr);
    while (std::getline(keyStream, token, ',')) {
        uniqueKeys.push_back(token);
    }
}

void Schema::saveToFile(const std::string& path) {
    std::ofstream out(path);
    for (size_t i = 0; i < fields.size(); ++i) {
        out << fields[i].type << " " << fields[i].name;
        if (i < fields.size() - 1)
            out << ", ";
    }
    out << "\n";

    for (size_t i = 0; i < uniqueKeys.size(); ++i) {
        out << uniqueKeys[i];
        if (i < uniqueKeys.size() - 1)
            out << ",";
    }
    out << "\n";
}

std::vector<Schema::Field> Schema::getFields() const {
    return fields;
}



std::vector<std::string> Schema::getUniqueKeys() const {
    // std::cout << "returining unique keys" << std::endl;
    // for (auto e : uniqueKeys) std::cout << e << std::endl;
    return uniqueKeys;
}

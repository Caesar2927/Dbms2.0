#pragma once
#include <string>
#include <vector>
#include <utility>

namespace Utils {
    std::string trim(const std::string& str);
    std::vector<std::string> split(const std::string& s, char delimiter);
    std::vector<std::pair<std::string, std::string>> parseSchema(const std::string& schemaString);
    bool isInteger(const std::string& s);
    bool isStringType(const std::string& s);
    bool fileExists(const std::string& path);
    bool folderExists(const std::string& path);
    void createFolder(const std::string& path);
}

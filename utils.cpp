#include "utils.h"
#include <algorithm>
#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include <filesystem>

namespace Utils {

    std::string trim(const std::string& str) {
        const char* whitespace = " \t\n\r";
        size_t start = str.find_first_not_of(whitespace);
        size_t end = str.find_last_not_of(whitespace);
        return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
    }

    std::vector<std::string> split(const std::string& s, char delimiter) {
        std::vector<std::string> tokens;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, delimiter)) {
            tokens.push_back(trim(item));
        }
        return tokens;
    }

    std::vector<std::pair<std::string, std::string>> parseSchema(const std::string& schemaString) {
        std::vector<std::pair<std::string, std::string>> schema;
        auto tokens = split(schemaString, ',');
        for (const std::string& token : tokens) {
            auto parts = split(token, ' ');
            if (parts.size() >= 2) {
                schema.push_back({ parts[1], parts[0] });
            }
        }
        return schema;
    }

    bool isInteger(const std::string& s) {
        return s == "int";
    }

    bool isStringType(const std::string& s) {
        return s.find("string") != std::string::npos;
    }

    bool fileExists(const std::string& path) {
        std::ifstream file(path);
        return file.good();
    }

    bool folderExists(const std::string& path) {
        return std::filesystem::exists(path) && std::filesystem::is_directory(path);
    }

    void createFolder(const std::string& path) {
        std::filesystem::create_directories(path);
    }

}

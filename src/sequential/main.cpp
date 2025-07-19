#include "record.hpp"
#include <iostream>
#include <filesystem>

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file> <num_records>\n";
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    size_t num_records = std::stoull(argv[3]);

    std::string home = std::getenv("HOME");
    std::string temp_dir = home + "/mergesort_project/data/temp";
    std::filesystem::create_directories(temp_dir);
    generateInputFile(input_file, num_records);
    auto temp_files = createSortedChunks(input_file);
    mergeChunks(temp_files, output_file);
    if (validateOutput(output_file)) {
        std::cout << "Sequential sorting successful\n";
    } else {
        std::cout << "Sequential sorting failed\n";
    }

    return 0;
}

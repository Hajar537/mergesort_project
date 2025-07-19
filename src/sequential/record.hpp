#ifndef RECORD_HPP
#define RECORD_HPP
#include <cstdint>
#include <string>
#include <vector>
#include <fstream>

constexpr size_t PAYLOAD_MAX = 1024;
constexpr size_t CHUNK_SIZE = 1ULL << 30; // 1 GB

struct Record {
    uint64_t key;
    uint32_t len;
    std::string payload;
    Record(uint64_t k, uint32_t l, const char* p) : key(k), len(l), payload(p, l) {}
    bool operator<(const Record& other) const { return key < other.key; }
    bool operator>(const Record& other) const { return key > other.key; }
};

void generateInputFile(const std::string& filename, size_t num_records);
size_t readChunk(std::ifstream& in, std::vector<Record>& records, size_t max_bytes);
void sortAndWriteChunk(const std::vector<Record>& records, const std::string& temp_file);
std::vector<std::string> createSortedChunks(const std::string& input_file);
void mergeChunks(const std::vector<std::string>& temp_files, const std::string& output_file);
bool validateOutput(const std::string& output_file);
#endif

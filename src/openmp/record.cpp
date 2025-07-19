#include "record.hpp"
#include <random>
#include <ctime>
#include <algorithm>
#include <queue>
#include <iostream>
#include <filesystem>

void generateInputFile(const std::string& filename, size_t num_records) {
    std::ofstream out(filename, std::ios::binary);
    if (!out) {
        std::cerr << "Error opening output file: " << filename << "\n";
        return;
    }
    std::mt19937_64 rng(time(nullptr));
    std::uniform_int_distribution<uint64_t> key_dist(0, std::numeric_limits<uint64_t>::max());
    std::uniform_int_distribution<uint32_t> len_dist(8, PAYLOAD_MAX);
    std::string temp_payload(PAYLOAD_MAX, 0);
    for (size_t i = 0; i < num_records; ++i) {
        uint64_t key = key_dist(rng);
        uint32_t len = len_dist(rng);
        for (uint32_t j = 0; j < len; ++j) {
            temp_payload[j] = static_cast<char>(rand() % 256);
        }
        out.write(reinterpret_cast<const char*>(&key), sizeof(key));
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        out.write(temp_payload.data(), len);
    }
}

size_t readChunk(std::ifstream& in, std::vector<Record>& records, size_t max_bytes) {
    records.clear();
    size_t total_bytes = 0;
    while (total_bytes < max_bytes && in) {
        uint64_t key;
        if (!in.read(reinterpret_cast<char*>(&key), sizeof(key))) break;
        uint32_t len;
        if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
        if (len < 8 || len > PAYLOAD_MAX) {
            std::cerr << "Invalid payload length\n";
            break;
        }
        std::string payload(len, 0);
        if (!in.read(payload.data(), len)) break;
        records.emplace_back(key, len, payload.data());
        total_bytes += sizeof(key) + sizeof(len) + len;
    }
    return total_bytes;
}

void sortAndWriteChunk(const std::vector<Record>& records, const std::string& temp_file) {
    std::vector<Record> sorted_records = records;
    std::sort(sorted_records.begin(), sorted_records.end());
    std::ofstream out(temp_file, std::ios::binary);
    for (const auto& rec : sorted_records) {
        out.write(reinterpret_cast<const char*>(&rec.key), sizeof(rec.key));
        out.write(reinterpret_cast<const char*>(&rec.len), sizeof(rec.len));
        out.write(rec.payload.data(), rec.len);
    }
}

std::vector<std::string> createSortedChunks(const std::string& input_file) {
    std::ifstream in(input_file, std::ios::binary);
    if (!in) {
        std::cerr << "Error opening input file: " << input_file << "\n";
        return {};
    }
    std::vector<std::string> temp_files;
    std::vector<Record> records;
    size_t chunk_num = 0;
    std::string home = std::getenv("HOME");
    std::string temp_dir = home + "/mergesort_project/data/temp";
    while (in) {
        size_t bytes_read = readChunk(in, records, CHUNK_SIZE);
        if (records.empty()) break;
        std::string temp_file = temp_dir + "/chunk_" + std::to_string(chunk_num++) + ".bin";
        sortAndWriteChunk(records, temp_file);
        temp_files.push_back(temp_file);
    }
    return temp_files;
}

struct MergeRecord {
    Record rec;
    size_t file_idx;
    bool operator>(const MergeRecord& other) const { return rec > other.rec; }
};

void mergeChunks(const std::vector<std::string>& temp_files, const std::string& output_file) {
    std::vector<std::ifstream> inputs(temp_files.size());
    for (size_t i = 0; i < temp_files.size(); ++i) {
        inputs[i].open(temp_files[i], std::ios::binary);
        if (!inputs[i]) {
            std::cerr << "Error opening temp file: " << temp_files[i] << "\n";
            return;
        }
    }
    std::ofstream out(output_file, std::ios::binary);
    if (!out) {
        std::cerr << "Error opening output file: " << output_file << "\n";
        return;
    }
    std::priority_queue<MergeRecord, std::vector<MergeRecord>, std::greater<MergeRecord>> heap;
    for (size_t i = 0; i < inputs.size(); ++i) {
        uint64_t key;
        uint32_t len;
        if (inputs[i].read(reinterpret_cast<char*>(&key), sizeof(key)) &&
            inputs[i].read(reinterpret_cast<char*>(&len), sizeof(len))) {
            std::string payload(len, 0);
            if (inputs[i].read(payload.data(), len)) {
                heap.push({Record(key, len, payload.data()), i});
            }
        }
    }
    while (!heap.empty()) {
        auto smallest = heap.top();
        heap.pop();
        out.write(reinterpret_cast<const char*>(&smallest.rec.key), sizeof(smallest.rec.key));
        out.write(reinterpret_cast<const char*>(&smallest.rec.len), sizeof(smallest.rec.len));
        out.write(smallest.rec.payload.data(), smallest.rec.len);
        size_t idx = smallest.file_idx;
        uint64_t key;
        uint32_t len;
        if (inputs[idx].read(reinterpret_cast<char*>(&key), sizeof(key)) &&
            inputs[idx].read(reinterpret_cast<char*>(&len), sizeof(len))) {
            std::string payload(len, 0);
            if (inputs[idx].read(payload.data(), len)) {
                heap.push({Record(key, len, payload.data()), idx});
            }
        }
    }
    for (auto& in : inputs) in.close();
    out.close();
    for (const auto& file : temp_files) {
        std::filesystem::remove(file);
    }
}

bool validateOutput(const std::string& output_file) {
    std::ifstream in(output_file, std::ios::binary);
    if (!in) {
        std::cerr << "Error opening output file for validation: " << output_file << "\n";
        return false;
    }
    uint64_t prev_key = 0;
    bool first = true;
    size_t record_count = 0;
    while (in) {
        uint64_t key;
        uint32_t len;
        if (!in.read(reinterpret_cast<char*>(&key), sizeof(key))) break;
        if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
        in.seekg(len, std::ios::cur);
        if (!in) break;
        if (!first && key < prev_key) {
            std::cerr << "Sorting error at record " << record_count << "\n";
            return false;
        }
        prev_key = key;
        first = false;
        ++record_count;
    }
    std::cout << "Validated " << record_count << " records\n";
    return true;
}

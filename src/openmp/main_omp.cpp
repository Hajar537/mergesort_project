#include <algorithm>
#include "record.hpp"
#include <iostream>
#include <filesystem>
#include <omp.h>
#include <limits>

// OpenMP version of createSortedChunks
std::vector<std::string> createSortedChunksOMP(const std::string& input_file, int num_threads) {
    std::ifstream in(input_file, std::ios::binary);
    if (!in) {
        std::cerr << "Error opening input file: " << input_file << "\n";
        return {};
    }

    // First pass: read all chunks sequentially (file I/O must be sequential)
    std::vector<std::vector<Record>> all_chunks;
    std::vector<Record> records;
    
    std::string home = std::getenv("HOME");
    std::string temp_dir = home + "/mergesort_project/data/temp";
    
    while (in) {
        size_t bytes_read = readChunk(in, records, CHUNK_SIZE);
        if (records.empty()) break;
        all_chunks.push_back(records);
    }
    in.close();
    
    std::cout << "Read " << all_chunks.size() << " chunks, sorting with " << num_threads << " threads...\n";
    
    std::vector<std::string> temp_files(all_chunks.size());
    
    // Parallel sorting and writing of chunks
    #pragma omp parallel for num_threads(num_threads) schedule(dynamic)
    for (size_t i = 0; i < all_chunks.size(); ++i) {
        // Sort the chunk
        std::sort(all_chunks[i].begin(), all_chunks[i].end());
        
        // Create temp file name
        std::string temp_file = temp_dir + "/chunk_omp_" + std::to_string(i) + ".bin";
        temp_files[i] = temp_file;
        
        // Write sorted chunk
        std::ofstream out(temp_file, std::ios::binary);
        for (const auto& rec : all_chunks[i]) {
            out.write(reinterpret_cast<const char*>(&rec.key), sizeof(rec.key));
            out.write(reinterpret_cast<const char*>(&rec.len), sizeof(rec.len));
            out.write(rec.payload.data(), rec.len);
        }
        out.close();
        
        #pragma omp critical
        {
            std::cout << "Thread " << omp_get_thread_num() << " completed chunk " << i << std::endl;
        }
    }
    
    return temp_files;
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file> <num_records> <num_threads>\n";
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    size_t num_records = std::stoull(argv[3]);
    int num_threads = std::stoi(argv[4]);

    std::cout << "=== OpenMP MergeSort Starting ===" << std::endl;
    std::cout << "Input: " << input_file << std::endl;
    std::cout << "Output: " << output_file << std::endl;
    std::cout << "Records: " << num_records << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;

    // Set OpenMP thread count
    omp_set_num_threads(num_threads);

    // Create temp directory
    std::string home = std::getenv("HOME");
    if (!home.empty()) {
        std::string temp_dir = home + "/mergesort_project/data/temp";
        std::filesystem::create_directories(temp_dir);
        std::cout << "Created temp directory: " << temp_dir << std::endl;
    } else {
        std::cerr << "HOME environment variable not set!" << std::endl;
        return 1;
    }

    try {
        // Generate input file only if it doesn't exist
        if (!std::filesystem::exists(input_file)) {
            std::cout << "Generating input file..." << std::endl;
            generateInputFile(input_file, num_records);
        } else {
            std::cout << "Using existing input file: " << input_file << std::endl;
        }
        std::cout << "Input file ready" << std::endl;

        // Create sorted chunks using OpenMP
        std::cout << "Creating sorted chunks with OpenMP..." << std::endl;
        auto temp_files = createSortedChunksOMP(input_file, num_threads);
        
        if (temp_files.empty()) {
            std::cerr << "No chunks created! Check for errors above." << std::endl;
            return 1;
        }
        std::cout << "Created " << temp_files.size() << " sorted chunks" << std::endl;

        // Merge chunks (sequential - same as other versions)
        std::cout << "Merging chunks..." << std::endl;
        mergeChunks(temp_files, output_file);
        std::cout << "Chunks merged successfully" << std::endl;

        // Validate output
        std::cout << "Validating output..." << std::endl;
        if (validateOutput(output_file)) {
            std::cout << "=== OpenMP sorting successful! ===" << std::endl;
        } else {
            std::cout << "=== OpenMP sorting failed validation! ===" << std::endl;
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception caught!" << std::endl;
        return 1;
    }

    return 0;
}


#include "../sequential/record.hpp"
#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <iostream>
#include <filesystem>
#include <memory>
#include <atomic>
#include <mutex>

class Emitter : public ff::ff_node {
public:
    Emitter(const std::string& input_file) : in(input_file, std::ios::binary) {
        if (!in) {
            std::cerr << "Error opening input file: " << input_file << "\n";
        }
    }
    
    void* svc(void*) override {
        std::vector<Record> records;
        size_t bytes_read = readChunk(in, records, CHUNK_SIZE);
        if (records.empty()) {
            return ff::FF_EOS;
        }
        auto chunk = new std::vector<Record>(std::move(records));
        return chunk;
    }
    
private:
    std::ifstream in;
};

class Worker : public ff::ff_node {
public:
    Worker(std::atomic<size_t>& counter) : chunk_counter(counter) {
        std::string home = std::getenv("HOME");
        temp_dir = home + "/mergesort_project/data/temp";
    }
    
    void* svc(void* task) override {
        auto records = static_cast<std::vector<Record>*>(task);
        size_t chunk_id = chunk_counter.fetch_add(1);
        std::string temp_file = temp_dir + "/chunk_ff_" + std::to_string(chunk_id) + ".bin";
        
        try {
            sortAndWriteChunk(*records, temp_file);
            delete records;
            return new std::string(temp_file);
        } catch (const std::exception& e) {
            std::cerr << "Worker error: " << e.what() << "\n";
            delete records;
            return nullptr;
        }
    }
    
private:
    std::atomic<size_t>& chunk_counter;
    std::string temp_dir;
};

class Collector : public ff::ff_node {
public:
    void* svc(void* task) override {
        if (task == nullptr) return ff::FF_GO_ON;
        
        auto temp_file = static_cast<std::string*>(task);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            temp_files.push_back(*temp_file);
        }
        delete temp_file;
        return ff::FF_GO_ON;
    }
    
    std::vector<std::string> get_temp_files() {
        std::lock_guard<std::mutex> lock(mutex_);
        return temp_files;
    }
    
private:
    std::vector<std::string> temp_files;
    std::mutex mutex_;
};

std::vector<std::string> createSortedChunksFF(const std::string& input_file, int num_workers) {
    std::atomic<size_t> chunk_counter{0};
    
    auto emitter = new Emitter(input_file);
    auto collector = new Collector();
    
    std::vector<ff::ff_node*> workers;
    for (int i = 0; i < num_workers; ++i) {
        workers.push_back(new Worker(chunk_counter));
    }
    
    ff::ff_farm farm;
    farm.add_emitter(emitter);
    farm.add_collector(collector);
    farm.add_workers(workers);
    
    std::cout << "Starting FastFlow farm with " << num_workers << " workers...\n";
    
    if (farm.run_and_wait_end() < 0) {
        std::cerr << "Error running FastFlow farm\n";
        // Clean up on error
        delete emitter;
        delete collector;
        for (auto* w : workers) delete w;
        return {};
    }
    
    std::cout << "FastFlow farm completed\n";
    
    // Get temp files before cleanup
    std::vector<std::string> temp_files = collector->get_temp_files();
    
    std::cout << "Collected " << temp_files.size() << " temp files\n";
    
    // Clean up
    delete emitter;
    delete collector;
    for (auto* w : workers) delete w;
    
    return temp_files;
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file> <num_records> <num_workers>\n";
        return 1;
    }
    
    std::string input_file = argv[1];
    std::string output_file = argv[2];
    size_t num_records = std::stoull(argv[3]);
    int num_workers = std::stoi(argv[4]);
    
    std::cout << "=== FastFlow MergeSort Starting ===" << std::endl;
    std::cout << "Input: " << input_file << std::endl;
    std::cout << "Output: " << output_file << std::endl;
    std::cout << "Records: " << num_records << std::endl;
    std::cout << "Workers: " << num_workers << std::endl;
    
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
        // Generate input file
        std::cout << "Generating input file..." << std::endl;
        generateInputFile(input_file, num_records);
        std::cout << "Input file generated successfully" << std::endl;
        
        // Create sorted chunks using FastFlow
        std::cout << "Creating sorted chunks with FastFlow..." << std::endl;
        auto temp_files = createSortedChunksFF(input_file, num_workers);
        
        if (temp_files.empty()) {
            std::cerr << "No chunks created! Check for errors above." << std::endl;
            return 1;
        }
        
        std::cout << "Created " << temp_files.size() << " sorted chunks" << std::endl;
        
        // Merge chunks
        std::cout << "Merging chunks..." << std::endl;
        mergeChunks(temp_files, output_file);
        std::cout << "Chunks merged successfully" << std::endl;
        
        // Validate output
        std::cout << "Validating output..." << std::endl;
        if (validateOutput(output_file)) {
            std::cout << "=== FastFlow sorting successful! ===" << std::endl;
        } else {
            std::cout << "=== FastFlow sorting failed validation! ===" << std::endl;
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


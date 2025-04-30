// File: FileManagerService.cpp

#include <winsock2.h>
#include <windows.h>
#include <tchar.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "../include/httplib.h"
#include <vector>
#include <thread>
#include <mutex>
#include <future>
#include <queue>
#include <algorithm>
#include <condition_variable>
#include <openssl/sha.h>
#include "../include/json.hpp"

namespace fs = std::filesystem;
using json = nlohmann::json;

// Constants
const size_t CHUNK_SIZE = 1024 * 1024; // 1MB
const std::string CHUNK_DIR = "chunks/";
const std::string META_DIR = "meta/";
const std::string CHUNK_REF_DIR = "chunk_refs/";

// Global Variables
std::mutex meta_mutex;
httplib::Server svr;

// Windows Service variables
SERVICE_STATUS serviceStatus = {0};
SERVICE_STATUS_HANDLE serviceStatusHandle = nullptr;
std::thread serverThread;
std::mutex server_mutex;
bool is_server_paused = false;

// Helper Functions
std::string compute_hash(const std::vector<char>& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((unsigned char*)data.data(), data.size(), hash);
    char hexstr[65];
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        sprintf(hexstr + i * 2, "%02x", hash[i]);
    hexstr[64] = 0;
    return std::string(hexstr);
}

void save_chunk(const std::string& hash, const std::vector<char>& data) {
    fs::create_directories(CHUNK_DIR);
    std::string path = CHUNK_DIR + hash;
    if (!fs::exists(path)) {
        std::ofstream out(path, std::ios::binary);
        out.write(data.data(), data.size());
    }
}

void increment_chunk_ref(const std::string& chunk_hash) {
    fs::create_directories(CHUNK_REF_DIR);
    std::string ref_path = CHUNK_REF_DIR + chunk_hash + ".ref";
    
    int count = 0;
    if (fs::exists(ref_path)) {
        std::ifstream in(ref_path);
        in >> count;
    }
    
    count++;
    std::ofstream out(ref_path);
    out << count;
}

bool decrement_chunk_ref(const std::string& chunk_hash) {
    std::string ref_path = CHUNK_REF_DIR + chunk_hash + ".ref";
    
    if (!fs::exists(ref_path)) return true;
    
    int count = 0;
    {
        std::ifstream in(ref_path);
        in >> count;
    }
    
    count--;
    
    if (count <= 0) {
        fs::remove(ref_path);
        return true;
    } else {
        std::ofstream out(ref_path);
        out << count;
        return false;
    }
}

void save_metadata(const std::string& filename, const json& metadata) {
    std::lock_guard<std::mutex> lock(meta_mutex);
    fs::create_directories(META_DIR);
    std::string meta_path = META_DIR + filename + ".json";
    
    // Handle existing metadata
    if (fs::exists(meta_path)) {
        json old_meta;
        std::ifstream in(meta_path);
        in >> old_meta;
        
        // Decrement old references
        if (old_meta.contains("chunks")) {
            for (const auto& hash : old_meta["chunks"]) {
                std::string chunk_hash = hash.get<std::string>();
                if (decrement_chunk_ref(chunk_hash)) {
                    fs::remove(CHUNK_DIR + chunk_hash);
                }
            }
        }
    }
    
    // Save new metadata
    std::ofstream meta(meta_path);
    meta << metadata.dump(4);
    
    // Increment new references
    if (metadata.contains("chunks")) {
        for (const auto& hash : metadata["chunks"]) {
            increment_chunk_ref(hash.get<std::string>());
        }
    }
}

class ThreadPool {
public:
    ThreadPool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });
                        if (this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F>
    auto enqueue(F&& f) -> std::future<typename std::result_of<F()>::type> {
        using return_type = typename std::result_of<F()>::type;
        auto task = std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));
        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers)
            worker.join();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};


ThreadPool read_pool(std::thread::hardware_concurrency()); // Global thread pool for reads
constexpr size_t PARALLEL_THRESHOLD = 8; // Use parallel if chunks > 8

void process_file_content(const std::string& filename, const std::string& content) {
    json metadata;
    metadata["filename"] = filename;
    metadata["chunks"] = json::array();

    ThreadPool pool(std::thread::hardware_concurrency());
    std::vector<std::future<std::string>> futures;

    size_t offset = 0;
    while (offset < content.size()) {
        size_t chunk_size = std::min<size_t>(CHUNK_SIZE, content.size() - offset);
        
        std::vector<char> chunk(content.begin() + offset, content.begin() + offset + chunk_size);
        offset += chunk_size;

        futures.push_back(pool.enqueue([chunk]() {
            std::string hash = compute_hash(chunk);
            save_chunk(hash, chunk);
            return hash;
        }));  // Added missing parenthesis here
    }

    for (auto& f : futures) {
        metadata["chunks"].push_back(f.get());
    }

    save_metadata(filename, metadata);
}

void StartServer() {
    // Ensure required directories exist
    fs::create_directories(CHUNK_DIR);
    fs::create_directories(META_DIR);
    fs::create_directories(CHUNK_REF_DIR);

    svr.new_task_queue = [] {
        return new httplib::ThreadPool(std::thread::hardware_concurrency());
    };

    svr.Get("/", [](const httplib::Request& req, httplib::Response& res) {
        res.set_content("Welcome to the File Manager Server!", "text/plain");
    });

    svr.Post("/upload", [](const httplib::Request& req, httplib::Response& res) {
        try {
            if (req.files.empty()) {
                res.status = 400;
                res.set_content("No files received.", "text/plain");
                return;
            }

            std::string result_message;
            bool all_success = true;

            for (const auto& [field_name, file] : req.files) {
                std::string filename = file.filename;
                std::string content = file.content;
                std::string meta_path = META_DIR + filename + ".json";

                {
                    std::lock_guard<std::mutex> lock(meta_mutex);
                    if (fs::exists(meta_path)) {
                        result_message += "File already exists (skipped): " + filename + "\n";
                        all_success = false;
                        continue;
                    }
                }

                try {
                    process_file_content(filename, content);
                    result_message += "File uploaded and processed: " + filename + "\n";
                } catch (const std::exception& e) {
                    result_message += "Processing failed for " + filename + ": " + e.what() + "\n";
                    all_success = false;
                }
            }

            res.status = all_success ? 200 : 207;
            res.set_content(result_message, "text/plain");

        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(std::string("Error: ") + e.what(), "text/plain");
        }
    });

    svr.Put(R"(/files/(.+))", [](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];
        std::string meta_path = META_DIR + filename + ".json";

        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("No file uploaded.", "text/plain");
            return;
        }

        try {
            auto file = req.get_file_value("file");
            process_file_content(filename, file.content);

            res.status = 200;
            res.set_content("File updated successfully: " + filename, "text/plain");

        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content("Error updating file: " + std::string(e.what()), "text/plain");
        }
    });

    svr.Get(R"(/files/(.+))", [](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];
        std::string meta_path = META_DIR + filename + ".json";
    
        if (!fs::exists(meta_path)) {
            res.status = 404;
            res.set_content("File not found", "text/plain");
            return;
        }
    
        std::ifstream meta_ifs(meta_path);
        json metadata;
        meta_ifs >> metadata;
    
        const auto& chunks = metadata["chunks"];
        size_t num_chunks = chunks.size();
        std::vector<char> full_data;
    
        // -------------------------------
        // Hybrid Sequential/Parallel Logic
        // -------------------------------
        if (num_chunks <= PARALLEL_THRESHOLD) {
            // Sequential read for small files
            for (const auto& chunk_hash : chunks) {
                std::string chunk_path = CHUNK_DIR + chunk_hash.get<std::string>();
                std::ifstream chunk_ifs(chunk_path, std::ios::binary);
                
                if (!chunk_ifs) {
                    res.status = 500;
                    res.set_content("Missing chunk: " + chunk_hash.get<std::string>(), "text/plain");
                    return;
                }
    
                std::vector<char> buffer(
                    (std::istreambuf_iterator<char>(chunk_ifs)),
                    std::istreambuf_iterator<char>()
                );
                full_data.insert(full_data.end(), buffer.begin(), buffer.end());
            }
        } else {
            // Parallel read for large files
            std::vector<std::future<std::pair<size_t, std::vector<char>>>> futures;
            
            // Enqueue tasks with chunk indices
            for (size_t i = 0; i < num_chunks; ++i) {
                std::string chunk_hash = chunks[i].get<std::string>();
                futures.push_back(read_pool.enqueue([i, chunk_hash]() {
                    std::string chunk_path = CHUNK_DIR + chunk_hash;
                    std::ifstream chunk_ifs(chunk_path, std::ios::binary);
                    
                    if (!chunk_ifs) {
                        throw std::runtime_error("Missing chunk: " + chunk_hash);
                    }
    
                    std::vector<char> buffer(
                        (std::istreambuf_iterator<char>(chunk_ifs)),
                        std::istreambuf_iterator<char>()
                    );
                    return std::make_pair(i, buffer);
                }));
            }
    
            // Collect and sort results
            try {
                std::vector<std::pair<size_t, std::vector<char>>> results;
                for (auto& future : futures) {
                    results.push_back(future.get());
                }
    
                std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });
    
                for (const auto& [index, chunk_data] : results) {
                    full_data.insert(full_data.end(), chunk_data.begin(), chunk_data.end());
                }
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(e.what(), "text/plain");
                return;
            }
        }
    
        res.set_content(std::string(full_data.begin(), full_data.end()), "application/octet-stream");
    });

    svr.Delete(R"(/files/(.+))", [](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];
        std::string meta_path = META_DIR + filename + ".json";

        try {
            std::lock_guard<std::mutex> lock(meta_mutex);
            if (!fs::exists(meta_path)) {
                res.status = 404;
                res.set_content("File not found", "text/plain");
                return;
            }

            json metadata;
            std::ifstream meta_file(meta_path);
            meta_file >> metadata;

            // Decrement references and remove chunks if needed
            if (metadata.contains("chunks")) {
                for (const auto& hash : metadata["chunks"]) {
                    std::string chunk_hash = hash.get<std::string>();
                    if (decrement_chunk_ref(chunk_hash)) {
                        fs::remove(CHUNK_DIR + chunk_hash);
                    }
                }
            }

            fs::remove(meta_path);
            res.status = 200;
            res.set_content("File deleted successfully: " + filename, "text/plain");

        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content("Error deleting file: " + std::string(e.what()), "text/plain");
        }
    });

    svr.listen("0.0.0.0", 8080);
}

void StopServer() {
    svr.stop();
    if (serverThread.joinable()) {
        serverThread.join();
    }
}

void PauseServer() {
    std::lock_guard<std::mutex> lock(server_mutex);
    if (is_server_paused) return;
    is_server_paused = true;
    svr.stop();
}

void ResumeServer() {
    std::lock_guard<std::mutex> lock(server_mutex);
    if (!is_server_paused) return;
    is_server_paused = false;
    
    if (serverThread.joinable()) {
        serverThread.join();
    }
    serverThread = std::thread(StartServer);
}

void WINAPI ServiceCtrlHandler(DWORD ctrlCode) {
    switch (ctrlCode) {
        case SERVICE_CONTROL_STOP:
            serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            StopServer();
            serviceStatus.dwCurrentState = SERVICE_STOPPED;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            break;

        case SERVICE_CONTROL_PAUSE:
            serviceStatus.dwCurrentState = SERVICE_PAUSE_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            PauseServer();
            serviceStatus.dwCurrentState = SERVICE_PAUSED;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            break;

        case SERVICE_CONTROL_CONTINUE:
            serviceStatus.dwCurrentState = SERVICE_CONTINUE_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            ResumeServer();
            serviceStatus.dwCurrentState = SERVICE_RUNNING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            break;

        default:
            break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPTSTR* argv) {
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE;
    serviceStatus.dwWin32ExitCode = 0;
    serviceStatus.dwServiceSpecificExitCode = 0;
    serviceStatus.dwCheckPoint = 0;
    serviceStatus.dwWaitHint = 0;

    serviceStatusHandle = RegisterServiceCtrlHandler(TEXT("FileManagerService"), ServiceCtrlHandler);
    if (!serviceStatusHandle) {
        return;
    }

    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    serverThread = std::thread(StartServer);

    while (serviceStatus.dwCurrentState == SERVICE_RUNNING || serviceStatus.dwCurrentState == SERVICE_PAUSED) {
        Sleep(1000);
    }
}

int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        {(LPSTR)TEXT("FileManagerService"), (LPSERVICE_MAIN_FUNCTION)ServiceMain},
        {nullptr, nullptr}
    };

    if (!StartServiceCtrlDispatcher(serviceTable)) {
        std::cerr << "Failed to start service control dispatcher" << std::endl;
        return 1;
    }

    return 0;
}
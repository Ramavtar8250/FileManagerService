// File: FileManagerService.cpp
// Full implementation with hash-based file IDs and Windows service

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
#include <chrono>
#include <iomanip>

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

    // Skip if already saved
    if (fs::exists(path)) return;

    // Use a temp file + atomic rename for safety
    std::string temp_path = path + ".tmp";
    {
        std::ofstream out(temp_path, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Failed to open chunk file for writing: " + temp_path);
        }
        out.write(data.data(), data.size());
        if (!out) {
            throw std::runtime_error("Failed to write data to chunk file: " + temp_path);
        }
    }

    // Atomic rename to final path
    fs::rename(temp_path, path);
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

void save_metadata(const std::string& file_id, const json& metadata) {
    std::lock_guard<std::mutex> lock(meta_mutex);
    fs::create_directories(META_DIR);
    std::string meta_path = META_DIR + file_id + ".json";
    
    if (fs::exists(meta_path)) {
        json old_meta;
        std::ifstream in(meta_path);
        in >> old_meta;
        
        if (old_meta.contains("chunks")) {
            for (const auto& hash : old_meta["chunks"]) {
                std::string chunk_hash = hash.get<std::string>();
                if (decrement_chunk_ref(chunk_hash)) {
                    fs::remove(CHUNK_DIR + chunk_hash);
                }
            }
        }
    }
    
    std::ofstream meta(meta_path);
    meta << metadata.dump(4);
    
    if (metadata.contains("chunks")) {
        for (const auto& hash : metadata["chunks"]) {
            increment_chunk_ref(hash.get<std::string>());
        }
    }
}

class ThreadPool {
public:
    explicit ThreadPool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        condition.wait(lock, [this] {
                            return stop || !tasks.empty();
                        });
                        if (stop && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
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

ThreadPool read_pool(std::thread::hardware_concurrency());
constexpr size_t PARALLEL_THRESHOLD = 8;

std::string process_file_content(const std::string& filename, const std::string& content) {
    std::vector<char> content_data(content.begin(), content.end());
    std::string file_id = compute_hash(content_data);

    json metadata;
    metadata["filename"] = filename;
    metadata["CID"]=file_id;
    metadata["chunks"] = json::array();

    ThreadPool pool(std::thread::hardware_concurrency());
    std::vector<std::future<std::string>> futures;

    size_t offset = 0;
    while (offset < content.size()) {
        size_t chunk_size = std::min<size_t>(CHUNK_SIZE, content.size() - offset);
        std::vector<char> chunk(content.begin() + offset, content.begin() + offset + chunk_size);
        offset += chunk_size;

        futures.push_back(pool.enqueue([chunk] {
            std::string hash = compute_hash(chunk);
            save_chunk(hash, chunk);
            return hash;
        }));
    }

    for (auto& f : futures) {
        metadata["chunks"].push_back(f.get());
    }

    save_metadata(file_id, metadata);
    return file_id;
}

void StartServer() {
    fs::create_directories(CHUNK_DIR);
    fs::create_directories(META_DIR);
    fs::create_directories(CHUNK_REF_DIR);

    svr.new_task_queue = [] { return new httplib::ThreadPool(std::thread::hardware_concurrency()); };

    svr.Get("/", [](const auto& req, auto& res) {
        res.set_content("File Manager Service v2.0 (Hash-Based ID System)", "text/plain");
    });

    svr.Post("/upload", [](const httplib::Request& req, httplib::Response& res) {
        try {
            if (req.files.empty()) {
                res.status = 400;
                res.set_content("No files received", "text/plain");
                return;
            }

            json response;
            bool all_success = true;

            for (const auto& [field_name, file] : req.files) {
                std::vector<char> content_data(file.content.begin(), file.content.end());
                std::string file_id = compute_hash(content_data);
                std::string meta_path = META_DIR + file_id + ".json";

                {
                    std::lock_guard<std::mutex> lock(meta_mutex);
                    if (fs::exists(meta_path)) {
                        response["duplicates"].push_back({
                            {"original_name", file.filename},
                            {"file_id", file_id}
                        });
                        all_success = false;
                        continue;
                    }
                }

                try {
                    std::string generated_id = process_file_content(file.filename, file.content);
                    response["uploaded"].push_back({
                        {"original_name", file.filename},
                        {"file_id", generated_id}
                    });
                } catch (const std::exception& e) {
                    response["errors"].push_back({
                        {"filename", file.filename},
                        {"error", e.what()}
                    });
                    all_success = false;
                }
            }

            res.status = all_success ? 200 : 207;
            res.set_content(response.dump(2), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(json{{"error", e.what()}}.dump(), "application/json");
        }
    });

    // Add this new route in the StartServer() function after existing routes
    svr.Put(R"(/files/([a-fA-F0-9]{64}))", [](const httplib::Request& req, httplib::Response& res) {
        try {
            std::string old_file_id = req.matches[1];
            std::string old_meta_path = META_DIR + old_file_id + ".json";
    
            std::cout << "[INFO] Update request received for file_id: " << old_file_id << std::endl;
    
            // Check if the file exists
            {
                std::lock_guard<std::mutex> lock(meta_mutex);
                if (!fs::exists(old_meta_path)) {
                    std::cerr << "[WARN] File not found: " << old_file_id << std::endl;
                    res.status = 404;
                    res.set_content(json{{"error", "File not found"}, {"file_id", old_file_id}}.dump(), "application/json");
                    return;
                }
            }
    
            std::vector<char> new_content_data;
            std::string uploaded_filename = "updated_file";
    
            if (!req.files.empty()) {
                const auto& file = req.files.begin()->second;
                new_content_data.assign(file.content.begin(), file.content.end());
                uploaded_filename = file.filename;
                std::cout << "[INFO] Multipart upload: " << uploaded_filename << ", size: " << new_content_data.size() << std::endl;
            } else if (!req.body.empty()) {
                new_content_data.assign(req.body.begin(), req.body.end());
                std::cout << "[INFO] Raw body upload, size: " << new_content_data.size() << std::endl;
            } else {
                res.status = 400;
                res.set_content(json{{"error", "No file content received for update"}}.dump(), "application/json");
                return;
            }
    
            std::string new_content(new_content_data.begin(), new_content_data.end());
            std::string new_file_id = compute_hash(new_content_data);
    
            if (new_file_id == old_file_id) {
                std::cout << "[INFO] No content change for file: " << old_file_id << std::endl;
                res.set_content(json{
                    {"message", "File content unchanged"},
                    {"file_id", old_file_id}
                }.dump(), "application/json");
                return;
            }
    
            std::cout << "[INFO] Processing new content with ID: " << new_file_id << std::endl;
            std::string processed_file_id = process_file_content(uploaded_filename, new_content);
    
            // Atomically update metadata
            {
                std::lock_guard<std::mutex> lock(meta_mutex);
    
                if (processed_file_id != old_file_id && fs::exists(old_meta_path)) {
                    try {
                        std::cout << "[INFO] Removing old metadata: " << old_meta_path << std::endl;
                        fs::remove(old_meta_path);
                    } catch (const fs::filesystem_error& e) {
                        std::cerr << "[ERROR] Failed to remove old metadata: " << e.what() << std::endl;
                    }
                }
            }
    
            res.set_content(json{
                {"message", "File updated successfully"},
                {"original_file_id", old_file_id},
                {"new_file_id", processed_file_id}
            }.dump(), "application/json");
    
        } catch (const std::exception& e) {
            std::cerr << "[CRITICAL] Update failed: " << e.what() << std::endl;
            res.status = 500;
            res.set_content(json{{"error", e.what()}}.dump(), "application/json");
        }
    });
    
    

    svr.Get(R"(/files/([a-fA-F0-9]{64}))", [](const httplib::Request& req, httplib::Response& res) {
        auto start = std::chrono::high_resolution_clock::now();
        std::string file_id = req.matches[1];
        std::string meta_path = META_DIR + file_id + ".json";

        if (!fs::exists(meta_path)) {
            res.status = 404;
            res.set_content(json{{"error", "File not found"}, {"file_id", file_id}}.dump(), "application/json");
            return;
        }

        try {
            json metadata;
            {
                std::ifstream meta_ifs(meta_path);
                meta_ifs >> metadata;
            }

            const auto& chunks = metadata["chunks"];
            std::vector<char> full_data;
            std::vector<std::future<std::pair<size_t, std::vector<char>>>> futures;

            for (size_t i = 0; i < chunks.size(); ++i) {
                std::string chunk_hash = chunks[i].get<std::string>();
                futures.push_back(read_pool.enqueue([i, chunk_hash] {
                    std::string chunk_path = CHUNK_DIR + chunk_hash;
                    std::ifstream chunk_ifs(chunk_path, std::ios::binary);
                    if (!chunk_ifs) throw std::runtime_error("Missing chunk: " + chunk_hash);
                    
                    std::vector<char> buffer(
                        (std::istreambuf_iterator<char>(chunk_ifs)),
                        std::istreambuf_iterator<char>()
                    );
                    return std::make_pair(i, buffer);
                }));
            }

            try {
                std::vector<std::pair<size_t, std::vector<char>>> results;
                for (auto& future : futures) results.push_back(future.get());
                
                std::sort(results.begin(), results.end());
                for (const auto& [idx, data] : results) {
                    full_data.insert(full_data.end(), data.begin(), data.end());
                }
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(json{{"error", e.what()}}.dump(), "application/json");
                return;
            }

            res.set_header("Content-Type", "application/octet-stream");
            res.set_header("Content-Disposition", 
                "attachment; filename=\"" + metadata["filename"].get<std::string>() + "\"");
            res.set_content(std::string(full_data.begin(), full_data.end()), "application/octet-stream");

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            res.set_header("X-Processing-Time", std::to_string(duration.count()) + "ms");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(json{{"error", e.what()}}.dump(), "application/json");
        }
    });

    // Add this new route in the StartServer() function after existing routes
    svr.Delete(R"(/files/([a-fA-F0-9]{64}))", [](const httplib::Request& req, httplib::Response& res) {
        std::string file_id = req.matches[1];
        std::string meta_path = META_DIR + file_id + ".json";
    
        try {
            std::unique_lock<std::mutex> lock(meta_mutex);
            
            // First check existence before opening
            if (!fs::exists(meta_path)) {
                res.status = 404;
                res.set_content(json{{"error", "File not found"}, {"file_id", file_id}}.dump(), "application/json");
                return;
            }
    
            // Read metadata in nested scope to ensure file closure
            json metadata;
            {
                std::ifstream meta_file(meta_path);
                if (!meta_file.is_open()) {
                    res.status = 500;
                    res.set_content(json{{"error", "Could not open metadata file"}}.dump(), "application/json");
                    return;
                }
                meta_file >> metadata;
            } // File stream closes here when leaving scope
    
            // Process chunk references
            if (metadata.contains("chunks")) {
                for (const auto& hash : metadata["chunks"]) {
                    std::string chunk_hash = hash.get<std::string>();
                    if (decrement_chunk_ref(chunk_hash)) {
                        fs::remove(CHUNK_DIR + chunk_hash);
                    }
                }
            }
    
            // Release lock before file deletion to allow full release
            lock.unlock();
    
            // Retry mechanism for file deletion
            int retries = 3;
            while (retries-- > 0) {
                try {
                    fs::remove(meta_path);
                    break;
                } catch (const fs::filesystem_error& e) {
                    if (retries == 0) throw;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
    
            res.set_content(json{{"message", "File deleted"}, {"file_id", file_id}}.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(json{{"error", e.what()}}.dump(), "application/json");
        }
    });

    svr.listen("0.0.0.0", 8080);
}

// Windows Service Control Functions
void WINAPI ServiceCtrlHandler(DWORD ctrlCode) {
    switch (ctrlCode) {
        case SERVICE_CONTROL_STOP:
            serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            svr.stop();
            if (serverThread.joinable()) serverThread.join();
            serviceStatus.dwCurrentState = SERVICE_STOPPED;
            break;

        case SERVICE_CONTROL_PAUSE:
            serviceStatus.dwCurrentState = SERVICE_PAUSE_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            svr.stop();
            serviceStatus.dwCurrentState = SERVICE_PAUSED;
            break;

        case SERVICE_CONTROL_CONTINUE:
            serviceStatus.dwCurrentState = SERVICE_CONTINUE_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            serverThread = std::thread(StartServer);
            serviceStatus.dwCurrentState = SERVICE_RUNNING;
            break;

        default:
            break;
    }
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
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
    if (!serviceStatusHandle) return;

    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    serverThread = std::thread(StartServer);
    serverThread.join();

    while (serviceStatus.dwCurrentState == SERVICE_RUNNING) {
        Sleep(1000);
    }
}

int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        {(LPSTR)TEXT("FileManagerService"), (LPSERVICE_MAIN_FUNCTION)ServiceMain},
        {nullptr, nullptr}
    };

    if (!StartServiceCtrlDispatcher(serviceTable)) {
        std::cerr << "Service control dispatcher failed: " << GetLastError() << std::endl;
        return 1;
    }
    return 0;
}   
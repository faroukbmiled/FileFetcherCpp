#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <boost/filesystem.hpp>
#include <mutex>
#include <atomic>
#include <future>
#include <thread>
#include <queue>

namespace fs = boost::filesystem;

std::mutex outputMutex;
std::atomic < int > logFileCounter(0);

class ThreadPool {
public: ThreadPool(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back([this]() {
            while (true) {
                std::function < void() > task;

                {
                    std::unique_lock < std::mutex > lock(queueMutex);
                    condition.wait(lock, [this]() {
                        return stop || !tasks.empty();
                        });

                    if (stop && tasks.empty()) {
                        return;
                    }

                    task = std::move(tasks.front());
                    tasks.pop();
                }

                task();
            }
            });
    }
}

      ~ThreadPool() {
          {
              std::unique_lock < std::mutex > lock(queueMutex);
              stop = true;
          }

          condition.notify_all();

          for (std::thread& thread : threads) {
              thread.join();
          }
      }

      template < class F,
          class...Args >
      auto enqueue(F&& f, Args && ...args) -> std::future < typename std::result_of < F(Args...) > ::type > {
          using return_type = typename std::result_of < F(Args...) > ::type;

          auto task = std::make_shared < std::packaged_task < return_type() >>(std::bind(std::forward < F >(f), std::forward < Args >(args)...));

          std::future < return_type > result = task->get_future(); {
              std::unique_lock < std::mutex > lock(queueMutex);

              if (stop) {
                  throw std::runtime_error("enqueue on stopped ThreadPool");
              }

              tasks.emplace([task]() {
                  (*task)();
                  });
          }

          condition.notify_one();
          return result;
      }

private: std::vector < std::thread > threads;
       std::queue < std::function < void() >> tasks;
       std::mutex queueMutex;
       std::condition_variable condition;
       bool stop;
};

void processFile(const std::string& filePath,
    const std::string& keyword,
    const std::string& folder) {
    const std::size_t bufferSize = 8192;

    std::ifstream inputFile(filePath, std::ios::binary);
    if (!inputFile) {
        std::cerr << "Error opening input file: " << filePath << '\n';
        return;
    }

    std::string logFileName = folder + "/log" + std::to_string(++logFileCounter) + ".txt";
    std::ofstream logFile(logFileName, std::ios_base::app);
    if (!logFile) {
        std::cerr << "Error opening log file: " << logFileName << '\n';
        inputFile.close();
        return;
    }

    std::string remainingLine;

    char buffer[bufferSize];
    while (inputFile.read(buffer, bufferSize)) {
        std::string chunk(buffer, inputFile.gcount());
        chunk = remainingLine + chunk;

        std::size_t pos = 0;
        std::size_t newlinePos;
        while ((newlinePos = chunk.find('\n', pos)) != std::string::npos) {
            std::string line = chunk.substr(pos, newlinePos - pos);
            if (!line.empty() && line.find(keyword) != std::string::npos) {
                std::lock_guard < std::mutex > lock(outputMutex);
                logFile << line << '\n';
            }
            pos = newlinePos + 1;
        }

        remainingLine = chunk.substr(pos);
    }

    inputFile.close();
    logFile.close();
}

void combineLogFiles(const std::string& keyword,
    const std::string& folder) {
    std::ofstream outputFile(folder + "/" + keyword + ".txt", std::ios_base::app);

    for (int i = 1; i <= logFileCounter; ++i) {
        std::string logFileName = folder + "/log" + std::to_string(i) + ".txt";
        std::ifstream logFile(logFileName);

        if (!logFile) {
            std::cerr << "Error opening log file: " << logFileName << '\n';
            continue;
        }

        std::string line;
        while (std::getline(logFile, line)) {
            std::lock_guard < std::mutex > lock(outputMutex);
            outputFile << line << '\n';
        }

        logFile.close();
        fs::remove(logFileName);
    }

    outputFile.close();
}

void removeDuplicatesParallel(const std::string& filePath) {
    std::ifstream inputFile(filePath);
    if (!inputFile) {
        std::cerr << "Error opening input file: " << filePath << '\n';
        return;
    }

    std::string tempFilePath = filePath + ".tmp";
    std::ofstream tempFile(tempFilePath);
    if (!tempFile) {
        std::cerr << "Error opening temporary file: " << tempFilePath << '\n';
        inputFile.close();
        return;
    }

    std::unordered_set < std::string > uniqueLines;

    std::string line;
    while (std::getline(inputFile, line)) {
        if (!line.empty())
            uniqueLines.insert(line);
    }

    inputFile.close();

    for (const auto& uniqueLine : uniqueLines) {
        tempFile << uniqueLine << '\n';
    }

    tempFile.close();
    fs::rename(tempFilePath, filePath);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage: program_name keyword folder\n";
        return 0;
    }

    std::string keyword = argv[1];
    std::string folder_name = argv[2];
    std::string outputFileName = folder_name + "/" + keyword + ".txt";

    const size_t numThreads = std::thread::hardware_concurrency();
    ThreadPool threadPool(numThreads);

    std::vector < std::future < void >> results;

    fs::directory_iterator end;
    for (fs::directory_iterator file(folder_name); file != end; ++file) {
        if (fs::is_regular_file(file->status()) && file->path().extension() == ".txt") {
            results.emplace_back(threadPool.enqueue(processFile, file->path().string(), keyword, folder_name));
        }
    }

    for (auto& result : results) {
        result.get();
    }

    combineLogFiles(keyword, folder_name);

    removeDuplicatesParallel(outputFileName);

    std::cout << "Processing complete. File name => " << outputFileName << '\n';

    return 0;
}

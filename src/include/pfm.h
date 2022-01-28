#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <cstring>

namespace PeterDB {

    typedef unsigned PageNum;
//    typedef int RC;
    enum RC {
        RBFM_EOF = -1,
        ok = 0,
        OPEN_FILE_FAIL = 1,
        CREA_FILE_FAIL = 2,
        REMV_FILE_FAIL = 3,
        CLOS_FILE_FAIL = 4,
        FD_FAIL = 5,
        OUT_OF_PAGE = 6,
        OOUT_SLOT = 7,
        RECORD_TOO_BIG = 8,
        RECORD_HAS_DEL = 9,
    };

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment
        bool is_file_exist(const char *filename);

    private:
        std::string FilePath;
        std::string FileAuxPath;
        unsigned HiddenPage;
    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned totalPage;
        std::vector<unsigned short> freeSpaceList;
        FILE *fd;
        FILE *fd_aux;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
        unsigned HiddenPage;
    };

} // namespace PeterDB

#endif // _pfm_h_
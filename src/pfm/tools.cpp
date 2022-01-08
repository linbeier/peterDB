//
// Created by 晓斌 on 1/5/22.
//

#include "src/include/pfm.h"

bool PeterDB::PagedFileManager::is_file_exist(const char *fileName)
{
    std::ifstream infile(fileName);
    return infile.good();
}
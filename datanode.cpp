//
// Created by 杜清鹏 on 2021/4/6.
//

#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging

#include <FileSystemDN.h>

int main(){
    lrc::FileSystemDN fileSystemDn;
    fileSystemDn.Run();
    return 0;
}
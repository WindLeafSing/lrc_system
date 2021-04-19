//
// Created by 杜清鹏 on 2021/4/6.
//

#ifndef LRC_TOOLBOX_H
#define LRC_TOOLBOX_H

#include "devcommon.h"

namespace lrc{
    extern void RandomStripeGenerator(const std::string & srcpath,int blocks,int blocksize);


    //to avoid multiple definition when called in CN.cpp ...
    inline std::string uritoipaddr(const std::string & uri)
    {
        return std::string{uri.substr(0,uri.find(':'))};
    }
}

#endif //LRC_TOOLBOX_H

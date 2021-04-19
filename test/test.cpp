//
// Created by 杜清鹏 on 2021/4/8.
//

#include <random>
#include <set>
#include <bitset>
#include "combination_generator.h"
#include "devcommon.h"
#include "MetaInfo.h"
#include "erasurecoding/LRCCoder.h"
using namespace std;
std::vector<std::unordered_map<std::string,std::pair<int, bool>>>
placement_resolve(std::unordered_map<int, lrc::ClusterInfo> m_cluster_info, int k, int l, int g,
                  bool designed_placement = false) {

    static std::vector<std::unordered_set<int>> stripe_local_group_history;
    static int stripe_global_history = -1;

    std::vector<std::unordered_map<std::string, std::pair<int, bool>>> ret(3,
                                                                           std::unordered_map<std::string, std::pair<int, bool>>());
    std::vector<int> seq(m_cluster_info.size());

    if (stripe_local_group_history.empty()) {
        std::set<int> current_stripe_clusters;
        allrandom:
        //random
        //only maintain one cluster one local group
        int total_cluster = m_cluster_info.size();
        int local_group = l;

        if (total_cluster < local_group) {
            return ret;
        }
        //pick l cluster to hold
        std::iota(seq.begin(), seq.end(), 0);
        std::shuffle(seq.begin(), seq.end(), std::mt19937(std::random_device()()));
    } else {
        std::unordered_set<int> black_list;
        for (auto s : stripe_local_group_history) {
            black_list.merge(s);
        }
        black_list.insert(stripe_global_history);

        seq.clear();
        int candidate = 0;
        while (candidate < m_cluster_info.size() && seq.size() < l) {
            while (black_list.contains(candidate) && candidate++);//short circuit evaluation
            seq.push_back(candidate);
        }


        std::shuffle(seq.begin(), seq.end(), std::mt19937(std::random_device()()));

        seq.push_back(stripe_global_history);
    }

    for (int i = 0; i < l; ++i) {
        int cluster_dn_num = m_cluster_info[seq[i]].datanodesuri.size();
        assert(cluster_dn_num > g);
        std::vector<int> dns(cluster_dn_num);
        std::iota(dns.begin(), dns.end(), 0);
        std::shuffle(dns.begin(), dns.end(), std::mt19937(std::random_device()()));
        for (int j = 0; j < g; ++j) {
            ret[0].insert({m_cluster_info[seq[i]].datanodesuri[dns[j]], {0, false}});
        }
        ret[1].insert({m_cluster_info[seq[i]].datanodesuri[dns[g]], {1, false}});
    }
    std::vector<int> global_parities(m_cluster_info[seq[l]].datanodesuri.size());
    std::iota(global_parities.begin(), global_parities.end(), 0);
    std::shuffle(global_parities.begin(), global_parities.end(), std::mt19937(std::random_device()()));
    assert(global_parities.size() >= g);
    for (int j = 0; j < g; ++j) {
        ret[2].insert(
                {m_cluster_info[seq[l]].datanodesuri[global_parities[j]], {2, false}});
    }

    if (stripe_local_group_history.empty()) {
        stripe_local_group_history.emplace_back(seq.begin(), seq.begin() + l);
        stripe_global_history = seq[l];
    } else {
        // if 2-merge pattern , then stripe history size up to 1
        stripe_local_group_history.clear();
        stripe_global_history = -1;
    }

    return ret;
}

int main() {

    std::vector<std::string> v1{"192.168.1.3:10001", "192.168.1.3:10002", "192.168.1.3:10003", "192.168.1.3:10004"};
    lrc::ClusterInfo c1(v1, std::string{"192.168.1.3:10001"}, 0, 0);
    std::vector<std::string> v2{"192.168.0.11:10001", "192.168.0.11:10002", "192.168.0.11:10003", "192.168.0.11:10004"};
    lrc::ClusterInfo c2(v2, std::string{"192.168.0.11:10001"}, 1, 0);
    std::vector<std::string> v3{"192.168.0.21:10001", "192.168.0.21:10002", "192.168.0.21:10003", "192.168.0.21:10004"};
    lrc::ClusterInfo c3(v3, std::string{"192.168.0.21:10001"}, 2, 0);
    std::vector<std::string> v4{"192.168.0.22:10001", "192.168.0.22:10002", "192.168.0.22:10003", "192.168.0.22:10004"};
    lrc::ClusterInfo c4(v4, std::string{"192.168.0.22:10001"}, 3, 0);


    std::unordered_map<int, lrc::ClusterInfo> m_cluster_info;
    m_cluster_info.insert({0, c1});
    m_cluster_info.insert({1, c2});
    m_cluster_info.insert({2, c3});
    m_cluster_info.insert({3, c4});
    {
        auto ret = placement_resolve(m_cluster_info, 2, 1, 2);

        for (auto d:ret[0]) {
            std::cout << "datanodes : " << d.first;

        }
        std::cout << "\n";

        for (auto lp:ret[1]) {
            std::cout << "lps : " << lp.first;

        }

        std::cout << "\n";

        for (auto gp:ret[2]) {
            std::cout << "gps : " << gp.first;

        }
        std::cout << "\n";

        ret = placement_resolve(m_cluster_info, 2, 1, 2);

        for (auto d:ret[0]) {
            std::cout << "datanodes : " << d.first;

        }
        std::cout << "\n";

        for (auto lp:ret[1]) {
            std::cout << "lps : " << lp.first;

        }

        std::cout << "\n";

        for (auto gp:ret[2]) {
            std::cout << "gps : " << gp.first;

        }
        std::cout << "\n";
    }



    {
        //test spdlog

        //test LRCCoder
        auto encoder = lrc::LRCCoder({8, 2, 4});
//        encoder.display_matrix();
        auto encoder1 = lrc::LRCCoder({6, 2, 3});
        encoder1.display_matrix();
        auto encoder3 = lrc::LRCCoder({12, 4, 3});
        encoder3.display_matrix();
        auto encoder2 = lrc::LRCCoder({4, 2,2});
        encoder2.display_matrix();
        auto encoder4 = lrc::LRCCoder({8, 4, 4});
        encoder4.display_matrix();
        int  matrix[4*4] = {1,2,3,1,4,5,1,16,17};
        std::cout << jerasure_invertible_matrix(matrix,3,8
                )<<std::endl;

        //test data
        auto data_k_ptrs = new char *[8];
        for(int i=0;i<8;++i)
        {
            data_k_ptrs[i]=new char[32*6];
            std::fill(data_k_ptrs[i],data_k_ptrs[i]+32*6,i<3?'a': (i % 2 ? 'b' : 'd'));
        }

        for(int i = 0 ; i < 8 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout << data_k_ptrs[i][j] <<(j<31?"\t":"\n");
            }
        }

        auto globalparity_g_ptrs = new char *[4];
        auto localparity_l_ptrs = new char*[2];

        for(int i=0;i<4;++i)
        {
            globalparity_g_ptrs[i]=new char[32*6];
            std::fill(globalparity_g_ptrs[i],globalparity_g_ptrs[i]+32*6,'1');
        }

        for(int i=0;i<2;++i)
        {
            localparity_l_ptrs[i]=new char[32*6];
            std::copy(data_k_ptrs[i*4],data_k_ptrs[i*4]+32*6,localparity_l_ptrs[i]);
        }

        for(int i =0;i<2;++i)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout  << localparity_l_ptrs[i][j] <<(j<31?"\t":"\n");
            }
        }

        std::cout << "encoding generator matrix :\n";
        encoder.display_matrix();
        encoder.encode(data_k_ptrs, localparity_l_ptrs,globalparity_g_ptrs,12,32*6);

        //display local and global parity
        std::cout << "local parity :\n";
        for(int i =0;i<2;++i)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout  << bitset<8>(localparity_l_ptrs[i][j]) <<(j<31?"\t":"\n");
            }
        }

        std::cout << "global parity :\n";
        for(int i = 0 ; i < 4 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout <<bitset<8>(globalparity_g_ptrs[i][j]) <<(j<31?"\t":"\n");
            }
        }



        //erasure 3 blocks

        std::fill(data_k_ptrs[0],data_k_ptrs[0]+32*6,'0');
        std::fill(data_k_ptrs[1],data_k_ptrs[1]+32*6,'0');
        std::fill(data_k_ptrs[2],data_k_ptrs[2]+32*6,'0');
//        std::fill(globalparity_g_ptrs[3],globalparity_g_ptrs[3]+32*6,'0');

        int erasures[5] = {0,1,2,-1,-1};

        //prints
        encoder.decode(8,4,12,0,erasures,data_k_ptrs,globalparity_g_ptrs,32*6);
        std::cout << " after decode : \n" ;
        for(int i = 0 ; i < 8 ;i++)
        {
            for(int j = 0;j<32;j++)
            {
                std::cout <<data_k_ptrs[i][j]<<(j<31?"\t":"\n");
            }
        }
    }
    return 0;
}

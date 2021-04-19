//
// Created by 杜清鹏 on 2021/3/29.
//

#include "FileSystemClient.h"
#include "ToolBox.h"
#include <iomanip>
#include <bitset>

using namespace std;

int main(){

/*
     {
         //test spdlog

         //test LRCCoder
         auto encoder = lrc::LRCCoder({8, 2, 4});


         //test data
         auto data_k_ptrs = new char *[8];
         for(int i=0;i<8;++i)
         {
             data_k_ptrs[i]=new char[32];
             std::fill(data_k_ptrs[i],data_k_ptrs[i]+32,i<3?'a': (i % 2 ? 'b' : 'd'));
         }

         for(int i = 0 ; i < 8 ;i++)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << bitset<8>(data_k_ptrs[i][j]) <<(j<31?"\t":"\n");
             }
         }

         auto globalparity_g_ptrs = new char *[4];
         auto localparity_l_ptrs = new char*[2];

         for(int i=0;i<4;++i)
         {
             globalparity_g_ptrs[i]=new char[32];
             std::fill(globalparity_g_ptrs[i],globalparity_g_ptrs[i]+32,'1');
         }

         for(int i=0;i<2;++i)
         {
             localparity_l_ptrs[i]=new char[32];
             std::copy(data_k_ptrs[i*4],data_k_ptrs[i*4]+32,localparity_l_ptrs[i]);
         }

         for(int i =0;i<2;++i)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout  << bitset<8>(localparity_l_ptrs[i][j]) <<(j<31?"\t":"\n");
             }
         }

         std::cout << "encoding generator matrix :\n";
         encoder.display_matrix();
         encoder.encode(data_k_ptrs, localparity_l_ptrs,globalparity_g_ptrs,16,32);

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

         std::fill(data_k_ptrs[2],data_k_ptrs[2]+32,'0');
         std::fill(data_k_ptrs[3],data_k_ptrs[3]+32,'0');
         std::fill(data_k_ptrs[6],data_k_ptrs[6]+32,'0');
         std::fill(globalparity_g_ptrs[3],globalparity_g_ptrs[3]+32,'0');

         int erasures[5] = {2,3,6,11,-1};

         //prints
         encoder.decode(8,4,16,0,erasures,data_k_ptrs,globalparity_g_ptrs,32);
         std::cout << " after decode : \n" ;
         for(int i = 0 ; i < 8 ;i++)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout <<bitset<8>(data_k_ptrs[i][j]) <<(j<31?"\t":"\n");
             }
         }
     }


*/

    {
        lrc::RandomStripeGenerator("teststripe1.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe2.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe3.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe4.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe5.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe6.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe7.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe8.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe9.txt", 2, 64 * 1024 * 1024);
        lrc::RandomStripeGenerator("teststripe10.txt", 2, 64 * 1024 * 1024);
        lrc::FileSystemClient fileSystemClient;
        fileSystemClient.UploadStripe("teststripe1.txt", 0, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe2.txt", 1, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe3.txt", 2, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe4.txt", 3, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe5.txt", 4, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe6.txt", 5, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe7.txt", 6, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe8.txt", 7, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe9.txt", 8, {2, 1, 2,64});
        fileSystemClient.UploadStripe("teststripe10.txt", 9, {2, 1, 2,64});

        auto stripelocs = fileSystemClient.ListStripes();
        for (auto stripe : stripelocs) {
            std::cout<<"stripeid: "<<stripe.stripeid<<std::endl;
            for (auto node : stripe.blklocation) {
                std::cout << (node=="\n" ? "":node) << "\t";
            }
        }
        std::cout << std::endl;

        fileSystemClient.DownLoadStripe("","",0);

    }
        //get fs view
        //fileSystemClient.ListStripes();

    return 0;
}
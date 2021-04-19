//
// Created by 杜清鹏 on 2021/4/1.
//

#include "FileSystemClient.h"
#include "coordinator.grpc.pb.h"
#include <boost/thread/barrier.hpp>
#include <boost/thread/thread.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/connect.hpp>
#include <asio/write.hpp>
#include <asio/read.hpp>
#include "ToolBox.h"

namespace lrc{
    bool FileSystemClient::UploadStripe(const std::string & srcpath,int stripeid, const ECSchema &ecschema) {
        grpc::ClientContext getlocationclientContext;
        coordinator::StripeInfo stripeInfo;
        stripeInfo.set_stripeid(stripeid);
        stripeInfo.set_stripe_k(ecschema.datablk);
        stripeInfo.set_stripe_l(ecschema.localparityblk);
        stripeInfo.set_stripe_g(ecschema.globalparityblk);
        coordinator::StripeLocation stripeLocation;
        auto res = m_fileSystem_ptr->uploadStripe(&getlocationclientContext,stripeInfo,&stripeLocation);

        if(!res.ok())
        {
            m_client_logger->error("upload stripe failed!");
            return false;
        }else{
            if(ecschema.datablk!=stripeLocation.dataloc_size()||
            ecschema.localparityblk!=stripeLocation.localparityloc_size()||
            ecschema.globalparityblk!=stripeLocation.globalparityloc_size())
            {
                m_client_logger->error("cluster configuration error!");
                return false;
            }
            /*
             * debug : response datanodes uri
             * */
            std::cout << "datanode : \n";
            for(auto d : stripeLocation.dataloc()){
                std::cout << d<<" ";
            }
            std::cout << "\n";
            std::cout << "localparitynode : \n";
            for(auto d : stripeLocation.localparityloc()){
                std::cout << d<<" ";
            }
            std::cout << "\n";
            std::cout << "globalparitynode : \n";
            for(auto d : stripeLocation.globalparityloc()){
                std::cout << d<<" ";
            }
            std::cout << "\n";



            int srcfd = open(srcpath.c_str(),O_RDONLY);
            if(srcfd<0)
            {
                perror("open src file error!");
                return false;
            }
            std::cout << "open file success !\n";
            int defaultcellsize = m_default_blk_size*1024;
            char * total_workspace = new char[(ecschema.datablk+ecschema.localparityblk+ecschema.globalparityblk)*defaultcellsize];

            char **datablks=new char*[ecschema.datablk];
            char **localparityblks=new char*[ecschema.localparityblk];
            char **globalparityblks=new char *[ecschema.globalparityblk];

            int k=0;
            for(int i =0;i<ecschema.datablk;++i,++k) datablks[i]=&total_workspace[k*defaultcellsize];
            for(int i =0;i<ecschema.localparityblk;++i,++k) localparityblks[i]=&total_workspace[k*defaultcellsize];
            for(int i =0;i<ecschema.globalparityblk;++i,++k) globalparityblks[i]=&total_workspace[k*defaultcellsize];
            /* use sendfile to bypass user space buffer */

            boost::thread_group tp_reader;//workers transfer packet from src file to mem buffer
            boost::barrier cyclic_barrier(ecschema.datablk);


            auto readtask = [_stripelocation=stripeLocation,
                             _ecschema=ecschema,logger=m_client_logger,
                             cyclic_barrier=&cyclic_barrier,_srcfd=srcfd,chunklen=defaultcellsize,phase=1024]  (char ** _datablks,char ** _localparityblks,char ** _globalparityblks,char * dstbuffer,int startblockindex)
                    mutable {
                asio::io_context _ioc;
                asio::error_code ec;
                int cursor=0;
                int _datatransferport_offset = 22221 - 10001;
                std::vector<asio::ip::tcp::socket> sock_localparity;
                std::vector<asio::ip::tcp::socket> sock_globalparity;
                asio::ip::tcp::socket sock_data(_ioc);
                auto uri = _stripelocation.dataloc(startblockindex);
                auto ipaddr = uri.substr(0,uri.find(':'));
                short port=std::stoi(uri.substr(uri.find(':')+1));
                short _datatransferport = port + _datatransferport_offset;
                asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr),_datatransferport);
                sock_data.connect(ep);

                std::cout << "sock_data connected"<<std::endl;
                if(0==startblockindex){
                    for(int i =0;i<_ecschema.localparityblk;i++)
                    {

                        asio::ip::tcp::socket sock(_ioc);
                        auto uri = _stripelocation.localparityloc(i);
                        auto ipaddr = uri.substr(0,uri.find(':'));
                        short _port=std::stoi(uri.substr(uri.find(':')+1));

                        asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr),_port+_datatransferport_offset);

                        //blocking sync
                        sock.connect(ep);
                        sock_localparity.emplace_back(std::move(sock));
                    }
                    for(int i =0;i<_ecschema.globalparityblk;i++)
                    {
                        asio::ip::tcp::socket sock(_ioc);
                        auto uri = _stripelocation.globalparityloc(i);
                        auto ipaddr = uri.substr(0,uri.find(':'));
                        short _port=std::stoi(uri.substr(uri.find(':')+1));
                        asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr),_port+_datatransferport_offset);
                        //blocking sync
                        sock.connect(ep);
                        sock_globalparity.emplace_back(std::move(sock));
                    }
                }
                while(cursor<phase)
                {
                    //each phase read a cellsize then await for other worker reach the phase
                    //then thread 0 perform encoding and send to DNs
                    int readn = pread(_srcfd,dstbuffer,chunklen,startblockindex*(chunklen*phase)+cursor*chunklen);
                    if(readn<0)
                    {
                        perror("read src file error!");
                        logger->error("reading workerthread {} read src file failed!",startblockindex);
                        return ;
                    }else if(readn==0)
                    {

                        logger->error("reading workerthread {} terminate ahead!",startblockindex);
                    }else{
                        (*cyclic_barrier).count_down_and_wait();
                        //at this point, workers are all awake and thread 0 perform encoding and sending
                        if(0==startblockindex)
                        {
                            //thread 0 do its job

                            LRCCoder coder({_ecschema.datablk,_ecschema.localparityblk,_ecschema.globalparityblk});
                            coder.encode(_datablks,_localparityblks,_globalparityblks,16,chunklen);

                            for(int i=0;i<sock_localparity.size();++i)
                            {
                                asio::write(sock_localparity[i],asio::buffer(_localparityblks[i],chunklen),ec);
                            }

                            for(int i=0;i<sock_globalparity.size();++i)
                            {
                                asio::write(sock_globalparity[i],asio::buffer(_globalparityblks[i],chunklen),ec);
                            }
                        }


                        //blocking sync
                        asio::write(sock_data,asio::buffer(dstbuffer,chunklen),ec);

                        (*cyclic_barrier).count_down_and_wait();//wait until thread 0 complete its job
                        cursor++;
                    }
                }
            };
            //post jobs
            std::cout << "about to post jobs ... \n";
            for(int i =0;i<ecschema.datablk;++i){
                tp_reader.create_thread(std::bind(readtask,datablks,localparityblks,globalparityblks,datablks[i],i));
            }
            /*encode src file and split into blocks file*/
            tp_reader.join_all();
            //check cn status
            std::cout << "jobs finished ... \n";
            grpc::ClientContext checkresultclientContext;
            coordinator::RequestResult checkres;


            auto checkstatus = m_fileSystem_ptr->uploadCheck(&checkresultclientContext,stripeInfo,&checkres);
            if(checkstatus.ok()&&checkres.trueorfalse())
            {
                m_client_logger->info("upload stripe success!");
                return true;
            }

            m_client_logger->error("upload stripe failed,please retry!");

            grpc::ClientContext rollbackctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            coordinator::RequestResult rollbackres;
            m_fileSystem_ptr->deleteStripe(&rollbackctx,stripeId,&rollbackres);
            return false;


        }


        return true;
    }

    std::vector<StripeInfo> FileSystemClient::ListStripes() const {
        std::vector<StripeInfo> ret;
        grpc::ClientContext lsctx;
        coordinator::ListAllStripeCMD cmd;
        //std::unique_ptr< ::grpc::ClientReader< ::coordinator::StripeLocation>>
        auto reader = m_fileSystem_ptr->listAllStripes(&lsctx,cmd);
        std::unique_ptr<coordinator::StripeLocation> stripeLocptr=std::make_unique<coordinator::StripeLocation>();
        int idx = 0;
        while(reader->Read(stripeLocptr.get())){
            StripeInfo stripeInfo;
            for(int i=0;i<stripeLocptr->dataloc_size();++i)
            {
                stripeInfo.blklocation.push_back(stripeLocptr->dataloc(i));
            }
            for(int i=0;i<stripeLocptr->localparityloc_size();++i)
            {
                stripeInfo.blklocation.push_back(stripeLocptr->localparityloc(i));
            }
            for(int i=0;i<stripeLocptr->globalparityloc_size();++i)
            {
                stripeInfo.blklocation.push_back(stripeLocptr->globalparityloc(i));
            }
            stripeInfo.stripeid=idx;
            idx++;
            ret.push_back(stripeInfo);
        }

        return ret;
    }

    bool FileSystemClient::DownLoadStripe(const std::string &srcpath, const std::string &dstpath, int stripe_id)
    {
        grpc::ClientContext downloadctx;
        coordinator::StripeId stripeId;
        stripeId.set_stripeid(stripe_id);
        coordinator::StripeLocation stripeLocation;
        auto status = m_fileSystem_ptr->downloadStripe(&downloadctx,stripeId,&stripeLocation);

        if(!status.ok())
        {
            m_client_logger->warn("download stripe {} failed!Please retry!",stripe_id);
            return false;
        }

        std::cout << "datanode : \n";
        for(auto d : stripeLocation.dataloc()){
            std::cout << d<<" ";
        }
        std::cout << "\n";
        std::cout << "localparitynode : \n";
        for(auto d : stripeLocation.localparityloc()){
            std::cout << d<<" ";
        }
        std::cout << "\n";
        std::cout << "globalparitynode : \n";
        for(auto d : stripeLocation.globalparityloc()){
            std::cout << d<<" ";
        }
        std::cout << "\n";



    boost::thread_group tp;
    char ** dataptrs=new char*[stripeLocation.dataloc_size()];
    for(int i=0;i<stripeLocation.dataloc_size();++i)
    {
        dataptrs[i]=new char[64*1024*1024];
    }

    asio::io_context ioc;
    auto readertask= [&](int idx){

        int datatransferport_offset = 12220;
        asio::ip::tcp::socket sock(ioc);
        auto uri = stripeLocation.dataloc(idx);
        auto ipaddr = uritoipaddr(uri);
        auto port = std::stoi(uri.substr(uri.find(':')+1));
        asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ipaddr),port+datatransferport_offset);
        sock.connect(ep);

        asio::read(sock,asio::buffer(dataptrs[idx],64*1024*1024));
        std::cout << "thread read a block!" <<std::endl;
    };

    for(int i =0;i<stripeLocation.dataloc_size();++i)
    {
        tp.create_thread([readertask, i] { return readertask(i); });
    }

    tp.join_all();

    if(!std::filesystem::exists("./download/"))
    {
        std::filesystem::create_directory("./download/");
    }

    std::ofstream ofs("./download/"+std::to_string(stripe_id),std::ios::binary|std::ios::trunc|std::ios::out);
    for(int i=0;i<stripeLocation.dataloc_size();++i)
    {
        ofs.write(dataptrs[i],64*1024*1024);
        ofs.flush();
    }
    std::cout << ofs.tellp()<<"bytes successfully downloaded!"<<std::endl;

    //otherwise ... try with hint
    return true;
    }

}
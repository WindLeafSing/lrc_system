//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_FILESYSTEMCN_H
#define LRC_FILESYSTEMCN_H

#include "coordinator.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "MetaInfo.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <spdlog/logger.h>
#include "devcommon.h"

namespace lrc {
    class FileSystemCN {
    public:



        FileSystemCN(const std::string &mConfPath = "./conf/configuration.xml",
                     const std::string &mMetaPath = "./meta/fsimage.xml",
                     const std::string &mLogPath = "./log/logFile.txt",
                     const std::string &mClusterPath = "./conf/cluster.xml");

        class FileSystemImpl final :public coordinator::FileSystem::Service{
            bool m_initialized{false};
        public:
            grpc::Status deleteStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                      ::coordinator::RequestResult *response) override;

        private:
            //logger itself is thread safe
            enum TYPE{
                DATA,LP,GP
            };


            std::shared_ptr<spdlog::logger> m_cn_logger;

            std::string m_conf_path;
            std::string m_meta_path;
            std::string m_log_path;
            std::string m_cluster_path;
            std::string m_fs_uri;

            ECSchema m_fs_defaultecschema;


            //performance issue to refactor as a rw mutex
            std::mutex m_fsimage_mtx;
            std::mutex m_transition_mtx;
            std::unordered_map<ECSchema,std::set<int>,ECSchemaHash,ECSchemaCMP> m_fs_stripeschema;//fsimage.xml
            std::unordered_map<int, std::vector<std::string>> m_fs_image;//read from meta[fsimage.xml] in initialize stage
            std::unordered_map<std::string, DataNodeInfo> m_dn_info;//cluster.xml
            std::unordered_map<int,ClusterInfo> m_cluster_info;


            std::mutex m_stripeuploadcount_mtx;
            std::condition_variable m_uploadingcond;
            std::unordered_map<int,int> stripe_in_uploadingcounter;
            std::mutex m_stripedownloadcount_mtx;
            std::condition_variable m_downloadingcond;
            std::unordered_map<int,int> stripe_in_downloadingcounter;
            std::unordered_map<int,std::unordered_map<std::string,std::pair<TYPE,bool>>> stripe_in_uploading;// stripeid , unreceivedDNList

            //stub
            std::map<std::string, std::unique_ptr<datanode::FromCoodinator::Stub>> m_dn_ptrs;

            bool askDNhandling(const std::string & dnuri,int stripeid);
        public:
            grpc::Status uploadCheck(::grpc::ServerContext *context, const::coordinator::StripeInfo *request,
                                     ::coordinator::RequestResult *response) override;

            FileSystemImpl()=default;


            grpc::Status listAllStripes(::grpc::ServerContext *context, const::coordinator::ListAllStripeCMD *request,
                                        ::grpc::ServerWriter<::coordinator::StripeLocation> *writer) override;

            void updatestripeuploadcounter(int stripeid,std::string fromdatanode);

            bool isMInitialized() const;

            const std::shared_ptr<spdlog::logger> &getMCnLogger() const;

            const std::string &getMFsUri() const;

            const ECSchema &getMFsDefaultecschema() const;


            const std::unordered_map<int, std::vector<std::string>> &getMFsImage() const;

            const std::unordered_map<std::string, DataNodeInfo> &getMDnInfo() const;

            FileSystemImpl(const std::string &mConfPath = "./conf/configuration.xml",
                           const std::string &mMetaPath = "./meta/fsimage.xml",
                           const std::string &mLogPath = "./log/logFile.txt",
                           const std::string &mClusterPath = "./conf/cluster.xml");


            grpc::Status createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                   ::coordinator::RequestResult *response) override;


            grpc::Status uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                      ::coordinator::StripeLocation *response) override;

            bool initialize();

            bool initcluster() ;

            bool clearexistedstripes();

            void loadhistory();

            grpc::Status transitionup(::grpc::ServerContext *context, const::coordinator::TransitionUpCMD *request,
                                      ::coordinator::RequestResult *response) override;

            grpc::Status reportblocktransfer(::grpc::ServerContext *context,const coordinator::StripeId *request,
                                             ::coordinator::RequestResult *response) override;

            void flushhistory();

            std::vector<std::unordered_map<std::string,std::pair<TYPE,bool>>> placement_resolve(ECSchema ecSchema,bool designed_placement=false);

            virtual ~FileSystemImpl();


            grpc::Status listStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                    ::coordinator::StripeLocation *response) override;


            grpc::Status downloadStripe(::grpc::ServerContext *context, const::coordinator::StripeId *request,
                                        ::coordinator::StripeLocation *response) override;

            std::vector<bool> checknodesalive(const std::vector<std::string> & vector);

            bool analysisdecodable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                   std::vector<std::string> lp,
                                   std::vector<bool> alivelp, std::vector<std::string> gp,
                                   std::vector<bool> alivegp);

            bool analysislocallyrepairable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                std::vector<std::string> lp,
                                                std::vector<bool> alivelp);

            bool dolocallyrepair(std::vector<std::string> dn, std::vector<bool> alivedn, std::vector<std::string> lp,
                                 std::vector<bool> alivelp);

            bool docompleterepair(std::vector<std::string> dn, std::vector<bool> alivedn, std::vector<std::string> lp,
                                  std::vector<bool> alivelp, std::vector<bool> gp, std::vector<bool> alivegp);

            bool askDNservepull(std::unordered_set<std::string> reqnodes, std::string src, int stripeid);

            grpc::Status
            downloadStripeWithHint(::grpc::ServerContext *context, const::coordinator::StripeIdWithHint *request,
                                   ::coordinator::StripeLocation *response) override;

            std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>>,
            std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
            generate_basic_transition_plan(const std::unordered_map<int, std::vector<std::string>>& fsimage);

            std::vector<std::tuple<int,int,std::vector<std::string>,std::vector<std::string>>>
            FileSystemCN::FileSystemImpl::generate_designed_transition_plan(const std::unordered_map<int, std::vector<std::string>> &fsimage);

        };



        class CoordinatorImpl final :public coordinator::FromDataNode::Service {

            std::shared_ptr<FileSystemImpl> m_fsimpl_ptr;
        public:
            const std::shared_ptr<FileSystemImpl> &getMFsimplPtr() const;

            void setMFsimplPtr(const std::shared_ptr<FileSystemImpl> &mFsimplPtr);

            CoordinatorImpl();

            CoordinatorImpl(const std::shared_ptr<FileSystemImpl> &mFsimplPtr) = delete;



        };


        void Run() {
            //need a builder
            std::string fsimpl_rpc_uri = m_fsimpl.getMFsUri();

            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(fsimpl_rpc_uri, grpc::InsecureServerCredentials());
            // Register "service" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *synchronous* service.
            builder.RegisterService(&m_fsimpl);
            // Finally assemble the server.
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            m_cn_logger->info("Server listening on {}", fsimpl_rpc_uri);

            // Wait for the server to shutdown. Note that some other thread must be
            // responsible for shutting down the server for this call to ever return.
            server->Wait();
        }

        bool isInitialzed() const{
            return m_initialized;
        }
    private:
        bool m_initialized{false};
//
//
//        std::string m_conf_path;
//        std::string m_meta_path;
//        std::string m_log_path;
//        std::string m_cluster_path;
//        std::string m_fs_uri;
//
//        ECSchema m_fs_defaultecschema;
//        std::unordered_set<std::string> m_fs_existeddir;
//        std::map<std::string,std::vector<StripeInfo>> m_fs_image;//read from meta[fsimage.xml] in initialize stage
//        std::map<std::string,DataNodeInfo> m_dn_info;

        //impl
        FileSystemImpl m_fsimpl;

        CoordinatorImpl m_cnimpl;
        std::shared_ptr<spdlog::logger> m_cn_logger;


        bool syncFileSystem() {
            //this func restore file system to last recorded history version
            //specifically , this function will enforce DNs to delete extra blocks
        }


    };


}


#endif //LRC_FILESYSTEMCN_H

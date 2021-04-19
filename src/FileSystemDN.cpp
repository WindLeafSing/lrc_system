//
// Created by 杜清鹏 on 2021/4/1.
//
#include <datanode.grpc.pb.h>
#include <FileSystemDN.h>
#include <asio.hpp>
#include <grpc++/create_channel.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace lrc {

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::handleblocktransfer(::grpc::ServerContext *context,
                                                                             const ::datanode::StripeId *request,

                                                                             ::datanode::RequestResult *response) {


        std::cout << "try prepare acceptor"<<std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder() ;

        auto acptptr = prepareacceptor(*this);

        if(acptptr) std::cout << "prepared acceptor"<<std::endl;
        else{
            std::cout << "prepared acceptor failed!"<<std::endl;
        }
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = 64 * 1024 * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        auto handler = [_sockptr = std::move(
                sockptr), _id = request->stripeid(), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize, int stripeid,std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            std::cout << "handle recv thread start"<<std::endl;
            ptr->accept(*_sockptr);
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            std::cout << "accept client connection" << std::endl;
            asio::read(*_sockptr, asio::buffer(buf,defaultblksize*1024*1024));
            std::cout << "receive a block" << std::endl;
            if(!std::filesystem::exists(std::filesystem::path{datadir})) std::filesystem::create_directory(datadir);
            std::cout << datadir+std::to_string(_id) <<std::endl;
            std::ofstream ofs(datadir+std::to_string(_id),std::ios::binary|std::ios::out|std::ios::trunc);
            ofs.write(&buf[0], 1024 * 1024 * 64);
            std::cout << "successfully write : " << ofs.tellp() <<"bytes" <<std::endl;
            ofs.flush();
            grpc::ClientContext reportctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            coordinator::RequestResult reportres;
            std::cout << "datanode call reportblktransfer to cn!\n";
            auto status = _this->m_fs_stub->reportblocktransfer(&reportctx, stripeId, &reportres);
            std::cout << "datanode call reportblktransfer to cn success!\n";
            if (status.ok() && reportres.trueorfalse()) {
                std::cout << "report stripe transfer reach cn! "<<std::endl;
                _this->m_dnfromcnimpl_logger->info("report stripe transfer reach cn!");

            } else {
                std::cout << "report stripe transfer not reach cn! "<<std::endl;
                _this->m_dnfromcnimpl_logger->error("report stripe transfer not reach cn!");
            }
        };

        try {
            std::thread h(std::move(handler), m_datapath, blocksize, request->stripeid(), holdme);
            h.detach();
        } catch (std::exception & e) {
            std::cout << e.what() << std::endl;
        }
        std::cout << "receive askDNhandling rpc!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::clearallstripe(::grpc::ServerContext *context,
                                                                        const ::datanode::ClearallstripeCMD *request,
                                                                        ::datanode::RequestResult *response) {

        //clear dir mdatapth
        m_dnfromcnimpl_logger->info("clear data directory!");
        std::filesystem::remove_all(m_datapath);
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl(
            const std::string &mConfPath,
            const std::string &mDatapath
    )
            : m_confpath(mConfPath), m_datapath(mDatapath) {

        m_dnfromcnimpl_logger = spdlog::basic_logger_mt("dncnimpl", "./log/logFile2.txt", true);

        std::cout << "hook——————————————————————————————————————————" << std::endl;
        if (!std::filesystem::exists(std::filesystem::path(m_datapath))) {
            std::cout << "hook——————————————————————————————————————————" << std::endl;
            std::filesystem::create_directory(std::filesystem::path(m_datapath));
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;

        auto res = initialize();
        std::cout << "hook——————————————————————————————————————————" << std::endl;

        if (!res) {
            m_dnfromcnimpl_logger->error("datanode fromcnimpl initialize failed!");
            return;
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;

        res = initstub();
        std::cout << "hook——————————————————————————————————————————" << std::endl;

        if (!res) {
            m_dnfromcnimpl_logger->error("datanode cnstub initialize failed!");
            return;
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;

        m_dnfromcnimpl_logger->info("datanode cnstub initialize success!");
        m_initialized = true;

    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDnfromcnUri() const {
        return m_dnfromcn_uri;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatatransferPort() const {
        return m_datatransfer_port;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatapath() const {
        return m_datapath;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                                   ::datanode::RequestResult *response) {

        m_dnfromcnimpl_logger->info("clear stripe {}", request->stripeid());
        std::filesystem::remove(m_datapath.append(std::to_string(request->stripeid())));
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::checkalive(::grpc::ServerContext *context,
                                                  const ::datanode::CheckaliveCMD *request,
                                                  ::datanode::RequestResult *response) {
        if (m_initialized) {
            response->set_trueorfalse(true);
        } else {
            response->set_trueorfalse(false);
        }
        return grpc::Status::OK;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnUri(const std::string &mDnfromcnUri) {
        m_dnfromcn_uri = mDnfromcnUri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatatransferPort(const std::string &mDatatransferPort) {
        m_datatransfer_port = mDatatransferPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatapath(const std::string &mDatapath) {
        m_datapath = mDatapath;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemDN::FromCoordinatorImpl::getMDnfromcnimplLogger() const {
        return m_dnfromcnimpl_logger;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnimplLogger(
            const std::shared_ptr<spdlog::logger> &mDnfromcnimplLogger) {
        m_dnfromcnimpl_logger = mDnfromcnimplLogger;
    }

    const asio::io_context &FileSystemDN::FromCoordinatorImpl::getMIoservice() const {
        return m_ioservice;
    }


    bool FileSystemDN::FromCoordinatorImpl::isInitialized() const {
        return m_initialized;
    }

    void FileSystemDN::FromCoordinatorImpl::setInitialized(bool initialized) {
        FromCoordinatorImpl::m_initialized = initialized;
    }

    bool FileSystemDN::FromCoordinatorImpl::initialize() {
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_confpath.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");

        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
            } else if (std::string{"datanodetransfer_port"} == propname) {
                m_datatransfer_port = propvalue;
            } else if (std::string{"default_block_size"} == propname) {
                m_defaultblocksize = std::stoi(propvalue);
            } else if (std::string{"datanode_uri"} == propname) {
                m_dnfromcn_uri = propvalue;
            }
        }

        return true;

    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMFsUri() const {
        return m_fs_uri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMFsUri(const std::string &mFsUri) {
        m_fs_uri = mFsUri;
    }

    int FileSystemDN::FromCoordinatorImpl::getMDefaultblocksize() const {
        return m_defaultblocksize;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDefaultblocksize(int mDefaultblocksize) {
        m_defaultblocksize = mDefaultblocksize;
    }

    bool FileSystemDN::FromCoordinatorImpl::initstub() {
        m_fs_stub = std::move(coordinator::FileSystem::NewStub(grpc::CreateChannel(
                m_fs_uri, grpc::InsecureChannelCredentials()
        )));
        m_cnfromdn_stub = std::move(coordinator::FromDataNode::NewStub(grpc::CreateChannel(
                m_fs_uri, grpc::InsecureChannelCredentials()
        )));

        return true;
    }

    std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> FileSystemDN::FromCoordinatorImpl::get_sharedholder() {
        return shared_from_this();

    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl() {

    }

    FileSystemDN::FromCoordinatorImpl::~FromCoordinatorImpl() {

    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::handleblockpull(::grpc::ServerContext *context, const::datanode::StripeId *request,
                                                       ::datanode::RequestResult *response) {
        std::cout << "try prepare acceptor"<<std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder() ;

        auto acptptr = prepareacceptor(*this);
        std::cout << "prepared acceptor"<<std::endl;

        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = 64 * 1024 * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        auto handler = [_sockptr = std::move(
                sockptr), _id = request->stripeid(), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize, int stripeid,std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            std::cout << "handle send thread start"<<std::endl;
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            ptr->accept(*_sockptr);
            std::cout << "accept client connection" << std::endl;
            if(!std::filesystem::exists(std::filesystem::path{datadir}))
            {
                _this->m_dnfromcnimpl_logger->warn("stripe not exists!");
                std::cout << "stripe not exists!"<<std::endl;
                return ;
            }
            std::ifstream ifs(datadir+std::to_string(_id),std::ios::binary|std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * 1024 * 64);
            std::cout << "successfully read : " << ifs.tellg() <<"bytes from " <<datadir+std::to_string(_id)<<std::endl;
            asio::write(*_sockptr, asio::buffer(buf,defaultblksize*1024*1024));
            std::cout << "successfully send !\n";

        };

        try {
            std::thread h(std::move(handler), m_datapath, blocksize, request->stripeid(), holdme);
            h.detach();
        } catch (std::exception & e) {
            std::cout << e.what() << std::endl;
        }
        std::cout<< "download prepared!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }


    FileSystemDN::~FileSystemDN() {

    }

    FileSystemDN::FileSystemDN(const std::string mConfPath, const std::string mLogPath,
                               const std::string mDataPath) : m_conf_path(mConfPath), m_log_path(mLogPath),
                                                              m_data_path(mDataPath),
                                                              m_dn_fromcnimpl_ptr(FileSystemDN::FromCoordinatorImpl::getptr(mConfPath, mDataPath)) {

        m_dn_logger = spdlog::basic_logger_mt("datanode_logger", mLogPath, false);
        m_datatransfer_port = m_dn_fromcnimpl_ptr->getMDatatransferPort();

    }
}
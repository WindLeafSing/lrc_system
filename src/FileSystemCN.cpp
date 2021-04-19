//
// Created by 杜清鹏 on 2021/4/1.
//

#include <spdlog/sinks/basic_file_sink.h>
#include "ToolBox.h"
#include "FileSystemCN.h"
#include "coordinator.grpc.pb.h"
#include "combination_generator.h"

namespace lrc {


    FileSystemCN::FileSystemCN(const std::string &mConfPath,
                               const std::string &mMetaPath,
                               const std::string &mLogPath,
                               const std::string &mClusterPath) : m_fsimpl(mConfPath, mMetaPath, mLogPath,
                                                                           mClusterPath) {
        m_cn_logger = m_fsimpl.getMCnLogger(); //must wait until impl initialized first!
        if (!m_fsimpl.isMInitialized()) {
            m_cn_logger->error("initialize FileSystemImpl failed!");
            return;
        }

        m_initialized = true;

    }


    grpc::Status
    FileSystemCN::FileSystemImpl::createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                            ::coordinator::RequestResult *response) {
        auto _dstpath = request->dstpath();

        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                               ::coordinator::StripeLocation *response) {
        std::unique_lock ulk2(m_fsimage_mtx);
        int stripeid = request->stripeid();

        std::vector<std::unordered_map<std::string, std::pair<TYPE, bool>>> cand;

        if (m_fs_image.count(stripeid)) {
            ulk2.unlock();
            goto addstripelocation;
            return grpc::Status::OK;
        } else {
            ulk2.unlock();
            std::unique_lock ulk1(m_stripeuploadcount_mtx);
            if (stripe_in_uploading.count(stripeid)) {
                ulk1.unlock();
                return grpc::Status::CANCELLED;
            }
            cand = placement_resolve({request->stripe_k(), request->stripe_l(), request->stripe_g(), 64});

            std::unordered_map<std::string, std::pair<TYPE, bool>> combined;
            for (int i = 0; i < cand.size(); ++i) {
                for (auto node:cand[i]) {
                    combined.insert(node);
                }
            }
            stripe_in_uploading.insert({stripeid, combined});

            stripe_in_uploadingcounter[stripeid] = stripe_in_uploading[stripeid].size();

        }

        m_cn_logger->info("picked datanodes for storing stripe {} :", stripeid);
        for (auto c:stripe_in_uploading[stripeid]) {
            m_cn_logger->info("{}", c.first);
        }
        addstripelocation:

        //test hook

//        askDNhandling("192.168.0.20:10001",0);



        for (auto p:cand[0]) {
            response->add_dataloc(p.first);
            std::cout << "try to adddataloc:" << p.first << std::endl;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }
        for (auto p:cand[1]) {
            response->add_localparityloc(p.first);
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;

        }
        for (auto p:cand[2]) {
            response->add_globalparityloc(p.first);
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;

        }


        return grpc::Status::OK;
    }


    FileSystemCN::FileSystemImpl::FileSystemImpl(const std::string &mConfPath, const std::string &mMetaPath,
                                                 const std::string &mLogPath,
                                                 const std::string &mClusterPath)
            : m_conf_path(mConfPath), m_meta_path(mMetaPath), m_log_path(mLogPath), m_cluster_path(mClusterPath) {

        m_cn_logger = spdlog::basic_logger_mt("cn_logger", mLogPath, true);

        if (!std::filesystem::exists(std::filesystem::path{m_conf_path})) {
            m_cn_logger->error("configure file not exist!");
            return;
        }


        auto init_res = initialize();
        if (!init_res) {
            m_cn_logger->error("configuration file error!");
            return;
        }

        auto cluster_res = initcluster();
        if (!cluster_res) {
            m_cn_logger->error("cluster file error!");
            return;
        }

        auto m_cluster_info_backup = m_cluster_info;
        /* init all stubs to dn */
        for (int i = 0; i < m_cluster_info_backup.size(); ++i) {

            auto dn_alive = std::vector<std::string>();
            for (auto p:m_cluster_info_backup[i].datanodesuri) {
                //filter out offline DNs
                //by call stubs checkalive
                auto _stub = datanode::FromCoodinator::NewStub(
                        grpc::CreateChannel(p, grpc::InsecureChannelCredentials()));

                int retry = 3;//default redetect 3 times
                while (0 != retry) {

                    grpc::ClientContext clientContext;
                    datanode::CheckaliveCMD Cmd;
                    datanode::RequestResult result;
                    grpc::Status status;

                    status = _stub->checkalive(&clientContext, Cmd, &result);


                    if (status.ok()) {
                        m_cn_logger->info("{} is living !", p);
                        if (!result.trueorfalse()) {
                            m_cn_logger->warn("but not initialized!");
                            // 3 * 10s is deadline
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            retry--;
                        } else {
                            dn_alive.push_back(p);
                            m_dn_ptrs.insert(std::make_pair(p, std::move(_stub)));
                            retry = 0;
                        }
                    } else {
                        m_dn_info.erase(p);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        retry--;
                    }

                }
            }
            if (!dn_alive.empty()) m_cluster_info[i].datanodesuri = dn_alive;
            else {
                //whole cluster offline
                m_cluster_info.erase(i);
            }
            //timeout,this node is unreachable ...
        }

        if (!std::filesystem::exists(std::filesystem::path{mMetaPath})) {
            auto clear_res = clearexistedstripes();
            if (!clear_res) {
                m_cn_logger->error("Metapath {} does not exists , clear file system failed!", mMetaPath);
                return;
            }
            //create mMetaPath
            std::filesystem::create_directory(std::filesystem::path{m_meta_path}.parent_path());

        } else {
//            loadhistory(); //todo
        }


        m_cn_logger->info("cn initialize success!");
        m_initialized = true;

    }

    bool FileSystemCN::FileSystemImpl::initialize() {
        /* set default ecschema initialize all DN stubs , load DN info Cluster info */
//parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_conf_path.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");

        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
            }
        }

        return true;


    }

    bool FileSystemCN::FileSystemImpl::initcluster() {

        //parse  cluster.xml
        try {
            pugi::xml_document xdoc;
            xdoc.load_file(m_cluster_path.c_str());
            auto clustersnode = xdoc.child("clusters");
            for (auto clusternode = clustersnode.child(
                    "cluster"); clusternode; clusternode = clusternode.next_sibling()) {

                auto id = clusternode.attribute("id").value();
                auto gatewayuri = clusternode.attribute("gateway").value();
                auto datanodes = clusternode.child("nodes");
                int cluster_id = std::stoi(id);
                DataNodeInfo dninfo;
                dninfo.clusterid = cluster_id;
                dninfo.clusterid = cluster_id;
                dninfo.crosscluster_routeruri = gatewayuri;

                std::vector<std::string> dns;
                for (auto eachdn = datanodes.first_child(); eachdn; eachdn = eachdn.next_sibling()) {
                    m_dn_info[eachdn.attribute("uri").value()] = dninfo;
                    dns.push_back(eachdn.attribute("uri").value());
                }
                ClusterInfo clusterInfo{dns, gatewayuri, cluster_id, 0};
                m_cluster_info[cluster_id] = clusterInfo;

            }
        } catch (...) {
            return false;
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::isMInitialized() const {
        return m_initialized;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemCN::FileSystemImpl::getMCnLogger() const {
        return m_cn_logger;
    }

    const std::string &FileSystemCN::FileSystemImpl::getMFsUri() const {
        return m_fs_uri;
    }

    const ECSchema &FileSystemCN::FileSystemImpl::getMFsDefaultecschema() const {
        return m_fs_defaultecschema;
    }


    const std::unordered_map<int, std::vector<std::string>> &FileSystemCN::FileSystemImpl::getMFsImage() const {
        return m_fs_image;
    }

    const std::unordered_map<std::string, DataNodeInfo> &FileSystemCN::FileSystemImpl::getMDnInfo() const {
        return m_dn_info;
    }

    bool FileSystemCN::FileSystemImpl::clearexistedstripes() {
        //call stubs to invoke DNs clear operation
        datanode::ClearallstripeCMD clearallstripeCmd;
        grpc::Status status;
        for (auto &stub:m_dn_ptrs) {
            grpc::ClientContext clientContext;
            datanode::RequestResult result;
            status = stub.second->clearallstripe(&clientContext, clearallstripeCmd, &result);
            if (status.ok()) {
                if (!result.trueorfalse()) {
                    m_cn_logger->error("{} clear all stripe failed!", stub.first);
                    return false;
                }
            } else {
                m_cn_logger->error("{} rpc error!", stub.first);
            }
        }
        return true;
    }

    void FileSystemCN::FileSystemImpl::loadhistory() {
        //parse /meta/fsimage.xml
        pugi::xml_document xdoc;
        xdoc.load_file("./meta/fsimage.xml");

        auto histnode = xdoc.child("history");
        if (!histnode) return;
        auto schemanode = histnode.child("ecschema");
        auto schemastr = std::string(schemanode.value());
        //k,l,g
        int k, l, g;
        int pos1 = 0, pos2 = 0;
        pos2 = schemastr.find(',', pos1);
        k = std::stoi(schemastr.substr(pos1, pos2 - pos1));
        pos1 = pos2;
        pos2 = schemastr.find(',', pos1);
        l = std::stoi(schemastr.substr(pos1, pos2));
        g = std::stoi(schemastr.substr(pos2));

        auto stripesnode = histnode.child("stripes");
        std::stringstream uri;
        for (auto stripenode = stripesnode.child("stripe"); stripenode; stripenode = stripenode.next_sibling()) {
            std::vector<std::string> _dns;
            int id = std::stoi(stripenode.attribute("id").value());
            std::string urilist = std::string(stripenode.attribute("nodes").value());
            int cursor = 0;
            while (cursor < urilist.size()) {
                if (urilist[cursor] != '#') {
                    uri.putback(urilist[cursor]);
                } else {
                    _dns.push_back(uri.str());
                    uri.clear();
                }
                cursor++;
            }
            m_fs_image.insert({id, _dns});
        }
    }


    void FileSystemCN::FileSystemImpl::updatestripeuploadcounter(int stripeid, std::string fromdatanode_uri) {
        std::scoped_lock lockGuard(m_stripeuploadcount_mtx, m_fsimage_mtx);
        std::cout << fromdatanode_uri << std::endl;

        // stripe_in_uploading[stripeid][fromdatanode_uri] = true; //this line truely distributed env only
        stripe_in_uploadingcounter[stripeid]--;
        if (stripe_in_uploadingcounter[stripeid] == 0)//all blks are ready in DN
        {
            if (m_fs_image.count(stripeid) == 0) {
                m_fs_image.insert({stripeid, std::vector<std::string>()});

                std::vector<std::string> datauris;
                std::vector<std::string> lpuris;
                std::vector<std::string> gpuris;
                for (auto &p :stripe_in_uploading[stripeid]) {
                    if (TYPE::DATA == p.second.first) {
                        datauris.push_back(p.first);
                    } else if (TYPE::LP == p.second.first) {
                        lpuris.push_back(p.first);
                    } else {
                        gpuris.push_back(p.first);
                    }
                }
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), datauris.begin(), datauris.end());
                m_fs_image[stripeid].push_back("d");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), lpuris.begin(), lpuris.end());
                m_fs_image[stripeid].push_back("l");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), gpuris.begin(), gpuris.end());
                m_fs_image[stripeid].push_back("g");//marker

                //associate schema to this stripe
                ECSchema ecSchema(datauris.size(), lpuris.size(), gpuris.size(), 64);
                m_fs_stripeschema[ecSchema].insert(stripeid);
            }
            stripe_in_uploading.erase(stripeid);
            stripe_in_uploadingcounter.erase(stripeid);
            m_uploadingcond.notify_all();
        }

    }

    void FileSystemCN::FileSystemImpl::flushhistory() {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        //flush back fs_image
        pugi::xml_document xdoc;
        auto rootnode = xdoc.append_child("history");
        auto ecschemanode = rootnode.append_child("ecschema");
        std::string ec{std::to_string(m_fs_defaultecschema.datablk)
                       + std::to_string(m_fs_defaultecschema.localparityblk)
                       + std::to_string(m_fs_defaultecschema.globalparityblk)};
        ecschemanode.set_value(ec.c_str());
        auto stripesnode = rootnode.append_child("stripes");
        std::string urilist;
        for (auto p:m_fs_image) {
            auto singlestripenode = stripesnode.append_child("stripe");
            singlestripenode.append_child("id").set_value(std::to_string(p.first).c_str());
            auto urilistsnode = singlestripenode.append_child("nodes");
            for (auto &nodeuri : p.second) {
                //dn1#dn2#...#*#lp1#lp2#...#*#gp1#gp2#...#*#
                urilist.append(("d" == nodeuri || "l" == nodeuri || "g" == nodeuri) ? "*" : nodeuri).append("#");
            }
            //throw away last"#"
            urilist.pop_back();
            singlestripenode.set_value(urilist.c_str());
            urilist.clear();
        }
        xdoc.save_file("./meta/fsimage.xml");
    }


    FileSystemCN::FileSystemImpl::~FileSystemImpl() {
        m_cn_logger->info("cn im-memory image flush back to metapath!");
        flushhistory();
    }

    std::vector<std::unordered_map<std::string, std::pair<FileSystemCN::FileSystemImpl::TYPE, bool>>>
    FileSystemCN::FileSystemImpl::placement_resolve(ECSchema ecSchema, bool designed_placement) {

        static std::vector<std::unordered_set<int>> stripe_local_group_history;
        static int stripe_global_history = -1;

        std::vector<std::unordered_map<std::string, std::pair<FileSystemCN::FileSystemImpl::TYPE, bool>>> ret(3,
                                                                                                              std::unordered_map<std::string, std::pair<TYPE, bool>>());
        std::vector<int> seq(m_cluster_info.size());

        if (stripe_local_group_history.empty()) {
            std::set<int> current_stripe_clusters;
            allrandom:
            //random
            //only maintain one cluster one local group
            int total_cluster = m_cluster_info.size();
            int local_group = ecSchema.localparityblk;

            if (total_cluster < local_group) {
                m_cn_logger->error("too few clusters!");
                return ret;
            }
            //pick l cluster to hold
            std::iota(seq.begin(), seq.end(), 0);
            std::random_shuffle(seq.begin(), seq.end());
        } else {
            std::unordered_set<int> black_list;
            for (auto s : stripe_local_group_history) {
                black_list.merge(s);
            }
            black_list.insert(stripe_global_history);

            seq.clear();

            int candidate = 0;
            while (candidate < m_cluster_info.size() && seq.size() < ecSchema.localparityblk) {
                if (black_list.contains(candidate)) {
                    candidate++;
                } else {
                    seq.push_back(candidate);
                }
            }


            std::random_shuffle(seq.begin(), seq.end());

            seq.push_back(stripe_global_history);
        }

        for (int i = 0; i < ecSchema.localparityblk; ++i) {
            int cluster_dn_num = m_cluster_info[seq[i]].datanodesuri.size();
            assert(cluster_dn_num > ecSchema.globalparityblk);
            std::vector<int> dns(cluster_dn_num);
            std::iota(dns.begin(), dns.end(), 0);
            std::random_shuffle(dns.begin(), dns.end());
            for (int j = 0; j < ecSchema.globalparityblk; ++j) {
                ret[0].insert({m_cluster_info[seq[i]].datanodesuri[dns[j]], {TYPE::DATA, false}});
            }
            ret[1].insert({m_cluster_info[seq[i]].datanodesuri[dns[ecSchema.globalparityblk]], {TYPE::LP, false}});
        }
        std::vector<int> global_parities(m_cluster_info[seq[ecSchema.localparityblk]].datanodesuri.size());
        std::iota(global_parities.begin(), global_parities.end(), 0);
        std::random_shuffle(global_parities.begin(), global_parities.end());
        assert(global_parities.size() >= ecSchema.globalparityblk);
        for (int j = 0; j < ecSchema.globalparityblk; ++j) {
            ret[2].insert(
                    {m_cluster_info[seq[ecSchema.localparityblk]].datanodesuri[global_parities[j]], {TYPE::GP, false}});
        }

        if (stripe_local_group_history.empty()) {
            stripe_local_group_history.push_back(
                    std::unordered_set<int>(seq.begin(), seq.begin() + ecSchema.localparityblk));
            stripe_global_history = seq[ecSchema.localparityblk];
        } else {
            // if 2-merge pattern , then stripe history size up to 1
            stripe_local_group_history.clear();
            stripe_global_history = -1;
        }

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
        return ret;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadCheck(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                              ::coordinator::RequestResult *response) {

        //handle client upload check
        //check if stripeid success or not
        std::unique_lock uniqueLock(m_stripeuploadcount_mtx, std::adopt_lock);
        //6s deadline
        auto res = m_uploadingcond.wait_for(uniqueLock, std::chrono::seconds(6), [&]() {
            return 0 == stripe_in_uploadingcounter.count(request->stripeid());
        });
        response->set_trueorfalse(res);

        if (res) flushhistory();
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::reportblocktransfer(::grpc::ServerContext *context,
                                                      const ::coordinator::StripeId *request,
                                                      ::coordinator::RequestResult *response) {
        m_cn_logger->info("datanode {} receive block of stripe {} from client successfully!", context->peer(),
                          request->stripeid());

        updatestripeuploadcounter(request->stripeid(), context->peer());

        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::askDNhandling(const std::string &dnuri, int stripeid) {
        m_cn_logger->info("ask {} to wait for client uploading!", dnuri);

        grpc::ClientContext handlectx;
        datanode::RequestResult handlereqres;
        datanode::StripeId stripeId;
        stripeId.set_stripeid(stripeid);
        auto status = m_dn_ptrs[dnuri]->handleblocktransfer(&handlectx, stripeId, &handlereqres);
        if (status.ok()) {
            return handlereqres.trueorfalse();
        } else {
            std::cout << "rpc askDNhandlestripe error!" << dnuri << std::endl;
            m_cn_logger->error("rpc askDNhandlestripe error!");
            return false;
        }


    }

    grpc::Status
    FileSystemCN::FileSystemImpl::deleteStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                               ::coordinator::RequestResult *response) {

        std::scoped_lock slk(m_stripeuploadcount_mtx, m_fsimage_mtx);
        for (auto dnuri:stripe_in_uploading[request->stripeid()]) {
            grpc::ClientContext deletestripectx;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(request->stripeid());
            datanode::RequestResult deleteres;
            m_dn_ptrs[dnuri.first]->clearstripe(&deletestripectx, stripeId, &deleteres);
        }
        //delete
        stripe_in_uploading.erase(request->stripeid());
        stripe_in_uploadingcounter.erase(request->stripeid());
        m_fs_image.erase(request->stripeid());
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::listStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                             ::coordinator::StripeLocation *response) {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        int stripeid = request->stripeid();

        auto &loc_str = m_fs_image[stripeid];
        int start = 0;
        for (int i = 0; i < 3; ++i) {
            while (start < loc_str.size() && "d" != loc_str[start]) {
                response->add_dataloc(loc_str[start]);
                start++;
            }
            start++;//skip "dlg"
            while (start < loc_str.size() && "l" != loc_str[start]) {
                response->add_localparityloc(loc_str[start]);
                start++;
            }
            start++;
            while (start < loc_str.size() && "g" != loc_str[start]) {
                response->add_globalparityloc(loc_str[start]);
                start++;
            }
        }


        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::downloadStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                                 ::coordinator::StripeLocation *response) {

        std::scoped_lock slk(m_fsimage_mtx);
        int stripeid = request->stripeid();
        std::vector<std::string> datauris;//extractdatablklocation(request->stripeid());
        std::vector<std::string> lpuris;//extractlpblklocation(request->stripeid());
        std::vector<std::string> gpuris;//extractgpblklocation(request->stripeid());
        goto serverequest;
        if (m_fs_image.contains(stripeid)) {
            //this a  valid case
            //check alive first , if necessary ,perform lrc decode
            //lazy repair ? eager repair !
            //once detected a failure , decode whole stripe and re-deploy
            //todo : backgroud repair and serve request first if possible

            int start = 0;
            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "d") {
                datauris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivedatanodes = checknodesalive(datauris);
            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "l") {
                lpuris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivelpnodes = checknodesalive(lpuris);

            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "g") {
                gpuris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivegpnodes = checknodesalive(datauris);

            //if it requires a repair

            if (std::find(alivedatanodes.begin(), alivedatanodes.end(), false) != alivedatanodes.end() &&
                std::find(alivelpnodes.begin(), alivelpnodes.end(), false) != alivelpnodes.end() &&
                std::find(alivegpnodes.begin(), alivegpnodes.end(), false) != alivegpnodes.end()) {
                //analysis
                bool decodable = analysisdecodable(datauris, alivedatanodes, lpuris, alivelpnodes, gpuris,
                                                   alivegpnodes);
                if (decodable) {
                    auto locallyrepairable = analysislocallyrepairable(datauris, alivedatanodes, lpuris, alivelpnodes);
                    if (locallyrepairable) {
                        bool res = dolocallyrepair(datauris, alivedatanodes, lpuris, alivelpnodes);
                    } else {
                        bool res = docompleterepair(datauris, alivedatanodes, lpuris, alivelpnodes, alivelpnodes,
                                                    alivegpnodes);
                    }



                    //refresh metadata
                } else {
                    m_cn_logger->warn("corrupted stripe {}", stripeid);
                    return grpc::Status::CANCELLED;
                }
            }


            //can serve download request


        } else {
            return grpc::Status::CANCELLED;
        }


        serverequest:
        int start = 0;
        while (start < m_fs_image[stripeid].size() &&
               m_fs_image[stripeid][start] != "d") {
            datauris.push_back(m_fs_image[stripeid][start]);
            start++;
        }
        //serve download
        bool res = askDNservepull(std::unordered_set<std::string>(datauris.cbegin(), datauris.cend()), "", stripeid);
        if (!res) {
            m_cn_logger->info("datanodes can not serve client download request!");
            return grpc::Status::CANCELLED;
        }

        for (int i = 0; i < datauris.size(); ++i) {
            response->add_dataloc(datauris[i]);
        }

        std::cout << "returned locations!\n";
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::analysisdecodable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                         std::vector<std::string> lp, std::vector<bool> alivelp,
                                                         std::vector<std::string> gp,
                                                         std::vector<bool> alivegp) {

        std::vector<int> remained(lp.size(), 0);
        int totalremain = 0;
        int r = gp.size();
        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            remained[i] = currentgroupneedrepair + (alivelp[i] ? -1 : 0);
            totalremain += remained[i];
        }

        if (totalremain <= r) {
            return true;
        } else {
            return false;
        }
    }

    bool
    FileSystemCN::FileSystemImpl::analysislocallyrepairable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                            std::vector<std::string> lp, std::vector<bool> alivelp) {
        int r = dn.size() / lp.size();

        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            currentgroupneedrepair += (alivelp[i] ? -1 : 0);
            if (0 != currentgroupneedrepair) {
                return false;
            }
        }

        return true;
    }

    bool FileSystemCN::FileSystemImpl::dolocallyrepair(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                       std::vector<std::string> lp, std::vector<bool> alivelp) {

        int r = dn.size() / lp.size();
        for (int i = 0; i < lp.size(); ++i) {
            std::unordered_set<std::string> excludednodes(dn.begin() + i * r,
                                                          dn.begin() + (i + 1) * r);//this local group
            excludednodes.insert(lp[i]);
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) {
                    excludednodes.erase(dn[i * r + j]);//exclude dead node
                    int clusterid = m_dn_info[dn[i * r + j]].clusterid;
                    //pick another dn
                    for (auto &cand:m_cluster_info[clusterid].datanodesuri) {
                        if (!excludednodes.contains(cand)) {
                            //
                            askDNservepull(excludednodes, uritoipaddr(cand), 0);
                            grpc::ClientContext localrepairctx;
                            datanode::NodesLocation stripeLocation;
                            datanode::RequestResult repairres;
                            auto status = m_dn_ptrs[cand]->dolocallyrepair(&localrepairctx, stripeLocation, &repairres);
                            if (status.ok()) {
                                return true;
                            } else {
                                //try another cand
                                ;
                            }
                        }

                    }

                    //fail
                    return false;
                }
            }
        }


    }

    bool FileSystemCN::FileSystemImpl::docompleterepair(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                        std::vector<std::string> lp, std::vector<bool> alivelp,
                                                        std::vector<bool> gp, std::vector<bool> alivegp) {


        return false;
    }

    bool FileSystemCN::FileSystemImpl::askDNservepull(std::unordered_set<std::string> reqnodes, std::string src,
                                                      int stripeid) {
        for (const auto &node:reqnodes) {
            grpc::ClientContext downloadctx;
            datanode::RequestResult res;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            std::cout << "ask datanode : " << node << " to serve client download request" << std::endl;
            auto status = m_dn_ptrs[node]->handleblockpull(&downloadctx, stripeId, &res);
            if (!status.ok()) {
                std::cout << " datanode :" << node << " no response ! try next ... " << std::endl;
                //maybe have a blacklist
                m_cn_logger->info("choosen datanode {} , can not serve download request!", node);
                return false;
            }
        }
        return true;

    }

    grpc::Status FileSystemCN::FileSystemImpl::listAllStripes(::grpc::ServerContext *context,
                                                              const ::coordinator::ListAllStripeCMD *request,
                                                              ::grpc::ServerWriter<::coordinator::StripeLocation> *writer) {
        std::scoped_lock slk(m_fsimage_mtx);
        for (int i = 0; i < m_fs_image.size(); ++i) {
            coordinator::StripeLocation stripeLocation;
            int j = 0;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("d" != m_fs_image[i][j]) {
                    stripeLocation.add_dataloc(m_fs_image[i][j]);
                    stripeLocation.add_dataloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));

                } else {
                    stripeLocation.add_dataloc("\n");
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("l" != m_fs_image[i][j]) {
                    stripeLocation.add_localparityloc(m_fs_image[i][j]);
                    stripeLocation.add_localparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    stripeLocation.add_localparityloc("\n");
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("g" != m_fs_image[i][j]) {
                    stripeLocation.add_globalparityloc(m_fs_image[i][j]);
                    stripeLocation.add_globalparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    stripeLocation.add_globalparityloc("\n");
                    break;
                }
            }
            writer->Write(stripeLocation);
        }

        return grpc::Status::OK;
    }

    std::vector<bool> FileSystemCN::FileSystemImpl::checknodesalive(const std::vector<std::string> &vector) {
        return std::vector<bool>();
    }

    grpc::Status FileSystemCN::FileSystemImpl::downloadStripeWithHint(::grpc::ServerContext *context,
                                                                      const ::coordinator::StripeIdWithHint *request,
                                                                      ::coordinator::StripeLocation *response) {
        return Service::downloadStripeWithHint(context, request, response);
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::transitionup(::grpc::ServerContext *context,
                                               const ::coordinator::TransitionUpCMD *request,
                                               ::coordinator::RequestResult *response) {
        std::scoped_lock slk(m_fsimage_mtx);
        //stop the world
        using ClusterLoc = std::tuple<int, int, int>;
        // {stripeid, fromnode, tonode}
        using MigrationPlan = std::vector<std::tuple<int, std::string, std::string>>;
        // {stripeid, fromnodes, codingnodes, tonodes}
        using XORCodingPlan = std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>;
        using LRCCodingPlan = std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>;
        coordinator::TransitionUpCMD_MODE mode = request->mode();
        if (mode == coordinator::TransitionUpCMD_MODE_BASIC) {
            //pick one cluster and one node as a gp-node in that cluster (simplified , just pick one from original g nodes in odd stripe)
            auto [migration_plans,coding_plans] = generate_basic_transition_plan(m_fs_image);

            for (const auto &stripe : coding_plans) {
                int stripe_id = std::get<0>(stripe);
                const std::vector<std::string> &fromuris = std::get<1>(stripe);
                const std::string &workinguri = std::get<2>(stripe);
                const std::vector<std::string> &touris = std::get<3>(stripe);

                datanode::OP op;
                for (const auto &from : fromuris) {
                    op.add_from(from);
                }
                for (const auto &to : touris) {
                    op.add_to(to);
                }
                op.set_op(datanode::OP_CODEC_LRC);

                grpc::ClientContext pppctx;
                datanode::RequestResult pppres;

                auto status = m_dn_ptrs[workinguri]->pull_perform_push(&pppctx, op, &pppres);
                if (!status.ok()) {
                    std::cout << workinguri << "perform ppp operation failed!\n";
                    m_cn_logger->error("perform ppp operation failed!\n");

                    return status;
                }
            }

            //migration
            for (const auto &nodepair : migration_plans) {
                int stripe_id = std::get<0>(nodepair);
                const auto &from = std::get<1>(nodepair);
                const auto &to = std::get<2>(nodepair);
                for(int j=0;j<from.size();++j){
                datanode::OP op;
                op.add_from(from[j]);
                op.set_op(datanode::OP_CODEC_NO);

                grpc::ClientContext askforhandlingctx;
                datanode::RequestResult askforhandlingres;
                datanode::StripeId stripeId;
                stripeId.set_stripeid(stripe_id);
                auto alivestatus = m_dn_ptrs[from[j]]->handleblockpull(&askforhandlingctx, stripeId, &askforhandlingres);
                if (!alivestatus.ok()) {
                    std::cout << from[j] << "can not handling pull request!\n";
                    m_cn_logger->error("{} can not handling pull request!", from[j]);
                    return alivestatus;
                }
                grpc::ClientContext pppctx;
                datanode::RequestResult pppres;
                m_dn_ptrs[to[j]]->pull_perform_push(&pppctx, op, &pppres);
                auto migrationstatus = m_dn_ptrs[from[j]]->handleblockpull(&askforhandlingctx, stripeId,
                                                                        &askforhandlingres);
                if (!migrationstatus.ok()) {
                    std::cout << from[j] << "can not handling pull request!\n";
                    m_cn_logger->error("{} can not handling pull request!", from[j]);
                    return migrationstatus;
                }
                }
            }
            return grpc::Status::OK;
        }/*else if(mode == coordinator::TransitionUpCMD_MODE_BASIC_PART){

            }*/else {
                const auto & coding_plans = generate_designed_transition_plan(m_fs_image);

        }


    }

    std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>>,
            std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
    FileSystemCN::FileSystemImpl::generate_basic_transition_plan(
            const std::unordered_map<int, std::vector<std::string>> &fsimage) {

        std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>> ret1;
        std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>> ret2;
        //todo load balance and priority schedule
        auto extractor = [&](const std::vector<std::string> &stripelocs) ->
                std::tuple<std::unordered_set<int>, std::unordered_set<int>, std::unordered_set<int>> {
            std::unordered_set<int> datacluster;
            std::unordered_set<int> globalcluster;
            bool flag = false;// following will be gp cluster
            int i = 0;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "d" && stripelocs[i] != "l") {

                    datacluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    if (stripelocs[i] == "l") break;
                    i++;
                    continue;
                }
            }
            i++;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "g") {
                    globalcluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    break;
                }
            }

            return {datacluster, {}, globalcluster};
        };

        int totalcluster = m_cluster_info.size();
        std::vector<int> total(totalcluster, 0);
        std::iota(total.begin(), total.end(), 0);
        for (int i = 1; i < fsimage.size(); ++i) {
            auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
            auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
            auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

            int k = kpos - fsimage[i - 1].cbegin();
            int l = lpos - kpos - 1;
            int g = gpos - lpos - 1;
            std::random_shuffle(total.begin(), total.end());
            auto[excluded, _ignore1, candg]=extractor(fsimage[i - 1]);
            auto[currentk, _ignore2, currentg]=extractor(fsimage[i]);
            std::vector<int> candcluster;//cand data cluster
            std::vector<int> overlap;
            std::vector<std::string> overlap_d_nodes;
            std::vector<std::string> overlap_l_nodes;
            std::vector<std::string> to_d_nodes;
            int u = 0;
            for (; u < k; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c)) {
                    overlap.push_back(c);
                    overlap_d_nodes.push_back(fsimage[i][u]);
                }
            }
            ++u;
            for (; fsimage[i][u] != "l"; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c)) {
                    overlap.push_back(c);
                    overlap_l_nodes.push_back(fsimage[i][u]);
                }
            }

            for (int j = 0; j < totalcluster && overlap.size() > candcluster.size(); ++j) {
                if (!excluded.contains(total[j]) && !candg.contains(total[j])) {
                    candcluster.push_back(i);
                }
            }

            //pick k datanodes l localparity nodes g globalparity nodes...
            //for this plan generator just pick 1 globalparity from candg , encoding, and forward to g-1 others
            auto target_g_cands = std::vector<std::string>(m_cluster_info[*candg.cbegin()].datanodesuri);
            std::random_shuffle(target_g_cands.begin(), target_g_cands.end());
            auto &target_coding_nodeuri = target_g_cands.front();
            std::vector<std::string> to_g_nodes(target_g_cands.cbegin() + 1, target_g_cands.cend());
            std::vector<std::string> from_d_nodes(fsimage[i - 1].cbegin(), kpos);
            from_d_nodes.insert(from_d_nodes.end(),
                                fsimage[i].cbegin(),
                                std::find(fsimage[i].cbegin(), fsimage[i].cend(), "d"));


            std::vector<std::string> overlap_nodes;
            int idx = 0;
            while (idx < overlap_l_nodes.size()) {
                overlap_nodes.insert(overlap_nodes.end(),
                                     overlap_d_nodes.begin() + idx * k,
                                     overlap_d_nodes.begin() + (idx + 1) * k);
                overlap_nodes.push_back(overlap_l_nodes[idx]);
                idx++;
                std::vector<std::string> thiscluster(m_cluster_info[candcluster[idx]].datanodesuri);
                std::random_shuffle(thiscluster.begin(), thiscluster.end());
                to_d_nodes.insert(to_d_nodes.end(), thiscluster.cbegin(),
                                  thiscluster.cbegin() + k + 1);//at lease k+1 nodes !
            }


            ret1.push_back(
                    std::make_tuple(i - 1, from_d_nodes, target_coding_nodeuri, to_g_nodes));
            ret2.push_back(
                    std::make_tuple(i - 1, overlap_nodes, to_d_nodes)
            );

            return {ret2, ret1};
        }
    }

    std::vector<std::tuple<int,int,std::vector<std::string>,std::vector<std::string>>>
    FileSystemCN::FileSystemImpl::generate_designed_transition_plan(const std::unordered_map<int, std::vector<std::string>> &fsimage){

    }







//CN in DN implementation
/*
grpc::Status FileSystemCN::CoordinatorImpl::reportblocktransfer(::grpc::ServerContext *context,
                                                                const ::coordinator::StripeId *request,
                                                                ::coordinator::RequestResult *response) {
    m_fsimpl_ptr->getMCnLogger()->info("datanode {} receive block of stripe {} from client!",context->peer(),request->stripeid());
    m_fsimpl_ptr->updatestripeuploadcounter(request->stripeid(),context->peer());//should be a synchronous method or atomic int;
    response->set_trueorfalse(true);
    return grpc::Status::OK;
}*/


    FileSystemCN::CoordinatorImpl::CoordinatorImpl() {}


    const std::shared_ptr<FileSystemCN::FileSystemImpl> &FileSystemCN::CoordinatorImpl::getMFsimplPtr() const {
        return m_fsimpl_ptr;
    }

    void FileSystemCN::CoordinatorImpl::setMFsimplPtr(const std::shared_ptr<FileSystemImpl> &mFsimplPtr) {
        m_fsimpl_ptr = mFsimplPtr;
    }


}
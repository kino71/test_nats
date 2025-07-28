#include <iostream>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <mutex>

#include "raiapi2.h"
#include "stream/io_stream.h" // rai::Sys::out
#include "stream/file_stream.h" // rai::Sys::out
#include "tibrv/tibrvcpp.h"

RaiQueue* queue;

 int dispatch() {
        for (;;) {
                queue->Dispatch();
        }
}

int main() {
        int status;     
        RaiApi* api = RaiApi::RaiOpen("tibrv", 0, nullptr);
    if (!api) std::cout << "RaiApi::RaiOpen error" << std::endl;
        std::cout << api->RaiVersion() << std::endl;
    rai::Sys::initialize();
        const char* service = "8550";
        const char* network = "";
        const char* daemon = "tcp:dev-raims:8550";
        // status = transport.create(service, network, daemon);
        // if (status != 0) std::cout << "TibrvNetTransport::create error" << std::endl;
        status = api->SetIoctl("service", service);
        if (status != 0) std::cout << "RaiApi::SetIoctl error" << std::endl;
        status = api->SetIoctl("network", network);
        if (status != 0) std::cout << "RaiApi::SetIoctl error" << std::endl;
        status = api->SetIoctl("daemon", daemon);
        if (status != 0) std::cout << "RaiApi::SetIoctl error" << std::endl;
    rai::Args args;
    api->GetArgs(args);
    api->ParseArgs(args);
    RaiSession* session = api->CreateSession();
    if (!session) std::cout << "RaiApi::CreateSession error" << std::endl;
    queue = session->CreateQueue(false);
        struct Callback : public RaiMsgCallback {
                void onMsg(RaiMsgEvent& event, RaiMsg& msg, void* closure) {
                        // std::cout << "receive" << std::endl;
                        msg.Print(rai::Sys::out);
                }
        } callback;
    std::thread(dispatch).detach();
        // transport.destroy();
        for (;;) {
        RaiSubscribe* subscription = queue->CreateSubscribe(&callback);
                if (!subscription) std::cout << "RaiQueue::CreateSubscribe error" << std::endl;
        subscription->Start("DDM.MTA.IT0005239360.MI", RaiSubscribe::BOTH);
                std::mutex mtx;
                std::unique_lock<std::mutex> lck(mtx);
                std::condition_variable cv;
                cv.wait_for(lck, std::chrono::duration<double>(1)); 
        }
        return 0;
}
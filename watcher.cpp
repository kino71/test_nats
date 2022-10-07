#include <sys/stat.h>
#include <sys/timeb.h>
#include <cstdio>
#include <memory.h>

#include <iostream>
#include <string>
#include <sstream>
#include <chrono>

#include <nats/nats.h>

static constexpr char const * const statusNames[] = {
    "NATS_CONN_STATUS_DISCONNECTED",
    "NATS_CONN_STATUS_CONNECTING",
    "NATS_CONN_STATUS_CONNECTED",
    "NATS_CONN_STATUS_CLOSED",
    "NATS_CONN_STATUS_RECONNECTING",
    "NATS_CONN_STATUS_DRAINING_SUBS",
    "NATS_CONN_STATUS_DRAINING_PUBS"
};

void now(char* buffer, std::size_t size)
{
	struct timeb clockb;
	ftime(&clockb);
	struct tm localtime;
	localtime_r(&clockb.time, &localtime);
	snprintf(buffer, size, "%4.4d/%2.2d/%2.2d %2.2d:%2.2d:%2.2d.%3.3d",
		 1900 + localtime.tm_year,
		 1 + localtime.tm_mon,
		 localtime.tm_mday,
		 localtime.tm_hour,
		 localtime.tm_min,
		 localtime.tm_sec,
		 clockb.millitm);
}
std::string getConnectedServer(natsConnection *nc) 
{
    char url[256]; 
    memset(url, 0, sizeof(url));

    natsConnection_GetConnectedUrl(nc, url, sizeof(url));

    return url;
}

void connectionStatusCB(natsConnection *nc, void *closure)
{
    natsConnStatus status = natsConnection_Status(nc);
    std::ostringstream out;
    out << "WARNING : [NATS] Server : " << getConnectedServer(nc) << "] ConnectionStatus " << status << " = " << statusNames[status] << " [closure=" << (closure ? "NOT " : "") << "NULL]"  << std::endl;
    std::cerr << out.str();
} 

void errHandler(natsConnection* nc, natsSubscription* sub, natsStatus s, void* closure) 
{
    natsStatus last;
    auto lastText = nats_GetLastError(&last);

    std::ostringstream out;
    out << "ERROR : [NATS Server : " << getConnectedServer(nc) << "]" 
	<< "[ConnectionStatus " << s << " = " << natsStatus_GetText(s) << "]" 
	<< "[LastError=" << last << " = " << lastText << "]" 
	<< "[closure=" << (closure ? "NOT " : "") << "NULL]" 
	<< std::endl;

    std::cerr << out.str();
}


natsStatus connection(natsConnection** nc, 
                      std::string const& name,  
                      std::string const& servers, 
                      int const timeoutInMillis, 
                      int const pingInterval, 
                      int const maxPingsOut, 
                      int const maxReconnect, 
                      int const reconnectWait)
{
    auto version = nats_GetVersion();
    std::cout << "NATS VERSION : " << version << std::endl;

    natsOptions* opts;

    // OPTIONS
    natsStatus s = natsOptions_Create(&opts); 
    if (NATS_OK != s)
    {
        return s;
    }
    int num = 1;

    char** c_servers = new char*[num];

    std::size_t b = 0;

    for (int k = 0; k < num ; ++k)
    {
        std::size_t e = servers.find(',', b);

        if (e == std::string::npos)
            e = servers.size();

        auto s = e - b;

        c_servers[k] = new char[s + 1];
        std::string server = servers.substr(b, s);

        strcpy(c_servers[k], server.c_str());

        b = e + 1;
    }
    natsOptions_SetServers(opts, const_cast<const char**>(c_servers), num); 
    if (NATS_OK != s)
    {
        return s;
    }	
    delete [] c_servers;

    s = natsOptions_SetNoRandomize(opts, true); 
    if (NATS_OK != s) // true: lista server da primo ad ultimo, false: random
    {
        return s;
    }	
    s = natsOptions_SetTimeout(opts, timeoutInMillis); 
    if (NATS_OK != s) // millis
    {
        return s;
    }		
    s = natsOptions_SetName(opts, name.c_str()); 
    if (NATS_OK != s)
    {
        return s;
    }
    natsOptions_SetVerbose(opts, true); 
    if (NATS_OK != s) // true: sends are echoed by the server with an `OK` protocol message
    {
        return s;
    }	
    s = natsOptions_SetPedantic(opts, true); if (NATS_OK != s) // true: some extra checks will be performed by the server
    {
        return s;
    }	
    s = natsOptions_SetPingInterval(opts, pingInterval); if (NATS_OK != s) // millis : ping pong timeout
    {
        return s;
    }	
    s = natsOptions_SetMaxPingsOut(opts, maxPingsOut); if (NATS_OK != s) // ping failed before reconnection (if allowed)
    {
        return s;
    }	
    s = natsOptions_SetAllowReconnect(opts, true); if (NATS_OK != s) // true : try to reconnect
    {
        return s;
    }	
    s = natsOptions_SetMaxReconnect(opts, maxReconnect); if (NATS_OK != s) // N : how many times try to reconnect
    {
        return s;
    }	
    s = natsOptions_SetReconnectWait(opts, reconnectWait); if (NATS_OK != s) // N : how many times to wait between reconnect
    {
        return s;
    }	
    // CALLBACKS
    s = natsOptions_SetReconnectedCB(opts, connectionStatusCB, NULL); if (NATS_OK != s)
    {
        return s;
    }	
    s = natsOptions_SetDisconnectedCB(opts, connectionStatusCB, NULL); if (NATS_OK != s) 
    {
        return s;
    }	
    s = natsOptions_SetClosedCB(opts, connectionStatusCB, NULL); if (NATS_OK != s) 
    {
        return s;
    }	
    s = natsOptions_SetErrorHandler(opts, errHandler, NULL); if (NATS_OK != s)
    {
        return s;
    }	
    //s = natsOptions_SetWriteDeadline(opts, 0);
    //if (NATS_OK != s)
    //{
    //    return s;
    //}	

    //  CONNECTION
    s = natsConnection_Connect(nc, opts); if (NATS_OK != s)
    {
        return s;
    } 
    return NATS_OK;
}


int main(int argc, char**argv)
{
    bool print = true;
    std::string servers = "nats://localhost:4222";
    std::string bucket;
    std::string keys;
    std::uint64_t size = 0L;

    //servers = "nats://msp_admin_devel:msp_admin_devel@vxr-dev-nats01:4222,nats://msp_admin_devel:msp_admin_devel@vxr-dev-nats02:4222,nats://msp_admin_devel:msp_admin_devel@vxr-dev-nats03:4222";
    servers = "nats://msp_admin_devel:msp_admin_devel@vxr-dev-nats01:4222";
    bucket = "ANAGRAFICA";

    std::cout << "cluster: " << servers
              << " bucket: " << bucket
              << " print: " << (print ? "yes" : "no") << std::endl;

    if (servers.size() == 0)
    {
        std::cerr << "ERROR : servers empty" << std::endl;
        return -1;
    }
    if (bucket.size() == 0)
    {
        std::cerr << "ERROR : bucket empty" << std::endl;
        return -1;
    }

    std::ostringstream name;
    name << "Watcher on bucket " << bucket << ":" << argv[1];
    std::cout << "Name : " << name.str() << std::endl;

    natsConnection* nc = nullptr;
/*
                      int const timeoutInMillis,
                      int const pingInterval,
                      int const maxPingsOut,
                      int const maxReconnect,
                      int const reconnectWait)
*/
    auto s = connection(&nc, name.str(), servers, 500, 20000, 100, 1000, 1000); 
    if (s != NATS_OK)
    {
        std::cerr << "ERROR : " << natsStatus_GetText(s) << std::endl;
        return -1;
    }
    jsCtx *js = nullptr;
    jsOptions jsOpts;  
    kvStore  *kv = nullptr;  

    s = jsOptions_Init(&jsOpts); 
    if (NATS_OK != s)
    {
        std::cerr << " : jsOptions_Init : " << natsStatus_GetText(s) << std::endl;
        return -1;
    }
    s = natsConnection_JetStream(&js, nc, &jsOpts);
    if (NATS_OK != s)
    {
        std::cerr << " : natsConnection_JetStream : " << natsStatus_GetText(s) << std::endl;
        return -1;
    }
    s = js_KeyValue(&kv, js, bucket.c_str()); 
    if (NATS_OK != s)
    {
        std::cerr << " : js_KeyValue : " << natsStatus_GetText(s) << std::endl;
        return -1;
    }


    kvWatcher* w = nullptr;
    kvWatchOptions o;
    kvWatchOptions_Init(&o);

        s = kvStore_WatchAll(&w, kv, &o); 
	if (NATS_OK != s)
        {
            std::cerr << "kvStore_Watch : " << natsStatus_GetText(s) << std::endl;
            return -1;
        }
    char buffer[64];

    auto start = std::chrono::system_clock::now();
    char start_time[64];
    now(start_time, 64);

    std::uint64_t counter;
    for (counter = 0L; (size == 0L) || counter < size;)
    {
        kvEntry* e = nullptr;

        s = kvWatcher_Next(&e, w, 100); 
        if (s == 17) 
        {
	    std::ostringstream out;
            out << "kvWatcher_Next : (" << s << ") : " << natsStatus_GetText(s) << std::endl;
	    std::cerr << out.str();
            break;
        }
        else if (s != NATS_OK) 
        {
	    std::ostringstream out;
            out << "kvWatcher_Next : (" << s << ") : " << natsStatus_GetText(s) << std::endl;
	    std::cerr << out.str();
            continue;
        }
        else if (!e)
        {
            //std::cerr << "kvWatcher_Next : entry is NULL" << std::endl;
            break;
        }
        if (print)
        {
            now(buffer, 64);

            const char* ptv = kvEntry_ValueString(e); if(ptv)
                printf("%s | %d | %s\n", buffer, counter, ptv);
            else 
                printf("%s | %d | NULL content\n", buffer, counter);

        }
	++counter;
        kvEntry_Destroy(e);        
    }
    auto end = std::chrono::system_clock::now();
    char end_time[64];
    now(end_time, 64);

    std::chrono::duration<double> elapsed = end-start;

    std::cout << "Started  at : " << start_time << std::endl;
    std::cout << "Finished at : " << end_time << std::endl;
    std::cout << "Got " << counter << " records in " <<  std::chrono::duration<double, std::milli>(elapsed).count() << "ms" << std::endl;

    kvWatcher_Destroy(w);
}

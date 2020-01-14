#include "connector.hpp"
#include <cstring>
#include <csignal>
#include "flow-messages-enriched.pb.h"

volatile sig_atomic_t signal_caught = 0;

void
signalhandler(int sig)
{
    signal_caught = sig;
}

int 
main()
{
    struct sigaction sigact;
    memset(&sigact, 0, sizeof(struct sigaction));
    sigact.sa_handler = signalhandler;
    if(sigaction(SIGINT, &sigact, 0) < 0) {
        std::cerr << "Could not set up signal handler." << std::endl; 
        return EXIT_FAILURE;
    }

    std::vector<std::string> topics{"flows-10109"};
    Connector test{};
    // TODO: set valid username and password
    // or use setAuthAnnon for access with limited rights.
    test.setAuth("username","password");
    // TODO: set valid broker address, topics and consumer group
    if(test.startConsumer("brokers", topics, "consumer group") < 0) {
        return EXIT_FAILURE;
    }

    flowmessageenriched::FlowMessage flowmsg{};

    // prints out the etype of each consumed flow until SIGINT signal has been received.
    while(!signal_caught) {
        std::shared_ptr<RdKafka::Message> msg = test.consume(1000);
        if(msg->len()) {
            std::string tmp_string{};
            tmp_string.resize(msg->len());
            memcpy((void*) &tmp_string[0], msg->payload(), msg->len());
            flowmsg.ParseFromString(tmp_string);

            uint32_t type = flowmsg.etype();
            std::cout << "FLOW TYPE: " << type << std::endl;
        }
    }
}

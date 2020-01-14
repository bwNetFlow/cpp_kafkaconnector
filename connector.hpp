#ifndef CONSUMER_HPP
#define CONSUMER_HPP

#include <iostream>
#include <memory>
#include <vector>
#include <openssl/x509.h>
#include <cstdint>

const int64_t OFFSET_END = -1;
const int64_t OFFSET_BEGINNING = -2;

const std::string get_default_ca_location();

class Connector
{
    public:
        Connector(std::string _security_protocol = "sasl_ssl", std::string _sasl_mechanism = "PLAIN", 
            std::string _ssl_ca_location = get_default_ca_location());

        ~Connector();

        void setAuthAnon();

        void setAuth(std::string _user, std::string _pass);

        int startConsumer(std::string _broker, std::vector<std::string> _topics, 
            std::string _consumer_group);

        void close();

        std::shared_ptr<RdKafka::Message> consume(int timeout_ms) const;
    private:
        class CRebalanceCb : public RdKafka::RebalanceCb {
            void rebalance_cb (RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                std::vector<RdKafka::TopicPartition*> &partitions);
        };

        int verify_credentials() const;
        int verify_user() const;
        int verify_pass() const;
        int verify_broker() const;
        int verify_topics() const;
        int verify_consumer_group() const;

        std::string security_protocol;
        std::string sasl_mechanism;
        std::string ssl_ca_location;

        std::string user;
        std::string pass;

        std::string broker;
        std::vector<std::string> topics;
        std::string consumer_group;

        std::string errstr;
        std::shared_ptr<RdKafka::Conf> global_conf;
        std::shared_ptr<RdKafka::Conf> topic_conf;
        std::shared_ptr<RdKafka::KafkaConsumer> consumer;
        std::shared_ptr<CRebalanceCb> reb_cb_obj;
};

#endif

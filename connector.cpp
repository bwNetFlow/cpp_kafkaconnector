#include "connector.hpp"

const std::string
get_default_ca_location()
{
    const char* dir;
    dir = getenv(X509_get_default_cert_dir_env());
    if(!dir) dir = X509_get_default_cert_dir();
    return std::string{dir};
}

Connector::Connector(std::string _security_protocol, std::string _sasl_mechanism, std::string _ssl_ca_location)
    : security_protocol{_security_protocol}, sasl_mechanism{_sasl_mechanism}, ssl_ca_location{_ssl_ca_location},
    user{}, pass{}, broker{}, topics{}, consumer_group{},
    errstr{}, global_conf{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)},
    topic_conf{RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)}, consumer{}, reb_cb_obj{}
    {
        std::cout << "Connector has been created..." << std::endl;
    }

Connector::~Connector()
{
    if(consumer) close();
    std::cout << "Connector has been destroyed..." << std::endl;
}

void
Connector::setAuthAnon()
{
    user = "anon";
    pass = "anon";
}

void
Connector::setAuth(std::string _user, std::string _pass)
{
    user = _user;
    pass = _pass;
}

int 
Connector::startConsumer(std::string _broker, std::vector<std::string> _topics, 
    std::string _consumer_group)
{
    broker = _broker;
    topics = _topics;
    consumer_group = _consumer_group;

    if(verify_credentials() < 0) return -1;

    if(global_conf->set("rebalance_cb", reb_cb_obj.get(), errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("sasl.username", user, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("sasl.password", pass, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("group.id", consumer_group, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("security.protocol", security_protocol, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("sasl.mechanisms", sasl_mechanism, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("ssl.ca.location", ssl_ca_location, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("bootstrap.servers", broker, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("auto.offset.reset", "smallest", errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    if(global_conf->set("default_topic_conf", topic_conf.get(), errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return -1;
    }

    consumer = std::shared_ptr<RdKafka::KafkaConsumer>{RdKafka::KafkaConsumer::create(global_conf.get(), errstr)};

    if(!consumer) {
        std::cerr << "Failed to initialize consumer itself: " << errstr << std::endl;
    //    this->~Connector();
        return -1;
    }

    std::cout << ">> Consumer " << user << " has been successfully initialized." << std::endl;

    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if(err) {
        std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
            << RdKafka::err2str(err) << std::endl;
        return -1;
    }

    return 0;
}

void
Connector::close()
{
    consumer->close();
}

std::shared_ptr<RdKafka::Message>
Connector::consume(int timeout_ms) const
{
    std::shared_ptr<RdKafka::Message> msg{consumer->consume(timeout_ms)};
    return msg;
}

void
Connector::CRebalanceCb::rebalance_cb (RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
    std::vector<RdKafka::TopicPartition*> &partitions)
{
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        for(auto const& value: partitions) {
            value->set_offset(RdKafka::Topic::OFFSET_END);
        }
        consumer->assign(partitions);
    } else {
        consumer->unassign();
    }
}

int
Connector::verify_credentials() const
{
    if(verify_user() < 0) return -1;
    if(verify_pass() < 0) return -1;
    if(verify_broker() < 0) return -1;
    if(verify_topics() < 0) return -1;
    if(verify_consumer_group() < 0) return -1;
    return 0;
}

int
Connector::verify_user() const
{
    if(user.size() == 0) {
        std::cerr << "ERROR: username was not specified and is empty" << std::endl;
        return -1;
    }
    return 0;
}

int
Connector::verify_pass() const
{
     if(pass.size() == 0) {
        std::cerr << "ERROR: password was not specified and is empty" << std::endl;
        return -1;
    }
    return 0; 
}

int 
Connector::verify_broker() const
{
    if(broker.size() == 0) {
        std::cerr << "ERROR: broker was not specified and is empty" << std::endl;
        return -1;
    }
    return 0;    
}

int 
Connector::verify_topics() const
{
    if(topics.size() == 0) {
        std::cerr << "ERROR: topics was not specified and is empty" << std::endl;
        return -1;
    }
    return 0; 
}


int 
Connector::verify_consumer_group() const
{
    if(consumer_group.size() == 0) {
        std::cerr << "ERROR: consumer group was not specified and is empty" << std::endl;
        return -1;
    }
    return 0; 
}

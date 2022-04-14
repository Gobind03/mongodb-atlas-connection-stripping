var config = {};
config.mongo = {};
config.mongo.uri = "mongodb+srv://admin:root@testingcluster.22txs.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
config.mongo.quota = {
    numPrimary: 1,
    numSecondary: 3,
};
config.mongo.max_quota_check_attempts = 10;

module.exports = config;
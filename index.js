const express = require('express');
const faker = require('faker');
const MongoClient = require('mongodb').MongoClient
const bodyParser = require('body-parser')
const ObjectID = require('mongodb').ObjectID;
const config = require('./config');
const StatsD = require('hot-shots');
const app = express();
const parseConnectionString = require('mongodb/lib/core').parseConnectionString;
const winston = require('winston');
const TestVersion = '1.2'

function getRandomSubarray(arr, size) {
    let shuffled = arr.slice(0), i = arr.length;
    let temp, index;
    while (i--) {
        index = Math.floor((i + 1) * Math.random());
        temp = shuffled[index];
        shuffled[index] = shuffled[i];
        shuffled[i] = temp;
    }
    return shuffled.slice(0, size);
}

const checkIsMasterResult = (url) => {
    // Set up the connection to the local db
    var mongoclient = new MongoClient(url, { native_parser: true });

    // Open the connection to the server
    return new Promise((resolve, reject) => {
        mongoclient.connect(function (err, mongoclient) {

            // Get the first db and do an update document on it
            var db = mongoclient.db("admin");
            db.command({ isMaster: 1 }).then(function (result, err) {
                if (err) reject(err);
                else resolve(result);
            });
        });
    });
}

const getSelectedNodes = (hosts, auth, selectionPoolSize, quota, polling_limit, polling_counter) => {
    // Selection Pool Sizing Check
    if (selectionPoolSize > hosts.length) selectionPoolSize = hosts.length;

    // Select random hosts using Fisher-Yates shuffle
    let randomHosts = getRandomSubarray(hosts, selectionPoolSize);

    // Generate the Promise Array
    let promises = [];
    for (var i = 0; i < randomHosts.length; i++) {
        // Create URI for isMaster execution
        let mongoUri = `mongodb://${auth.username}:${auth.password}@${randomHosts[i].host}:${randomHosts[i].port}/?authSource=${auth.db}&ssl=true`;

        // Get promise for isMaster
        promises.push(checkIsMasterResult(mongoUri));
    }

    let numPrimary = 0, numSecondary = 0;
    return new Promise((resolve, reject) => {
        Promise.all(promises).then(responses => {
            let final_hosts = [];
            responses.map((isMasterResponse, index) => {
                // Calculate primary hosts for quota check
                if (isMasterResponse.ismaster && numPrimary < quota.numPrimary) {
                    final_hosts.push(isMasterResponse.me);
                    numPrimary++;
                }
                // Calculate secondary hosts for quota check
                else {
                    if (numSecondary < quota.numSecondary) {
                        final_hosts.push(isMasterResponse.me);
                        numSecondary++;
                    }
                }
            });

            // Final Quota Check
            if (numPrimary >= quota.numPrimary && numSecondary >= quota.numSecondary) resolve(final_hosts);

            // Recursion in failure
            else {
                if (polling_counter != polling_limit) {
                    console.log("Quota Not Meet. Polling again...")
                    resolve(getSelectedNodes(hosts, auth, selectionPoolSize, quota, polling_limit, ++polling_counter));
                }
                else {
                    resolve(final_hosts);
                }
            }
        })
    });
}



const MESSAGE = Symbol.for('message');

const jsonFormatter = (logEntry) => {
    const base = { timestamp: new Date() };
    const json = Object.assign(base, logEntry)
    logEntry[MESSAGE] = JSON.stringify(json);
    return logEntry;
}

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format(jsonFormatter)(),
    transports: [new winston.transports.Console()],
});

const dogstatsd = new StatsD();

const dd_options = {
    'response_code': true,
    'tags': ['app:hello_node']
}

const connect_datadog = require('connect-datadog')(dd_options);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(connect_datadog);

// how many random mongos we will select from the SRV results
const numMongos = 4;
let totalNodesRequired = 0;

for (node_type in config.mongo.quota) {
    totalNodesRequired += config.mongo.quota[node_type];
}

const selectionPoolSize = 2 * totalNodesRequired

parseConnectionString(config.mongo.uri, {}, (err, parsed) => {
    if (err) {
        return console.log(err);
    }

    console.log("parsed: " + JSON.stringify(parsed));

    getSelectedNodes(parsed.hosts, parsed.auth, selectionPoolSize, config.mongo.quota,
        config.mongo.max_quota_check_attempts, 0)
        .then((selectedHosts) => {
            let mongoHosts = selectedHosts.join(',');
            //connect to subset of SRV hosts
            const mongoUri = `mongodb://${parsed.auth.username}:${parsed.auth.password}@${mongoHosts}/?authSource=${parsed.auth.db}&ssl=true`;
            const mongoClient = new MongoClient(mongoUri, { useUnifiedTopology: true });

            mongoClient.on('connectionPoolCreated', event => console.dir(event));
            mongoClient.on('connectionPoolClosed', event => console.dir(event));
            mongoClient.on('connectionCreated', event => console.dir(event));
            mongoClient.on('connectionReady', event => console.dir(event));
            mongoClient.on('connectionClosed', event => console.dir(event));
            mongoClient.on('connectionCheckOutStarted', event => console.dir(event));
            mongoClient.on('connectionCheckOutFailed', event => console.dir(event));
            mongoClient.on('connectionCheckedOut', event => console.dir(event));
            mongoClient.on('connectionCheckedIn', event => console.dir(event));
            mongoClient.on('connectionPoolCleared', event => console.dir(event));
            mongoClient.on('close', function (err) {
                if (!isClosing) {
                    console.trace('Lost connection to MongoDB uri:', uri, 'err:', err);
                    process.exit(1);
                }
            });

            mongoClient.on('serverDescriptionChanged', function (event) {
                logger.warn('MongoDB SDAM: serverDescriptionChanged:', event);
            });

            mongoClient.on('serverHeartbeatFailed', function (event) {
                logger.warn('MongoDB SDAM: serverHeartbeatFailed:', event);
            });

            mongoClient.on('serverOpening', function (event) {
                logger.warn('MongoDB SDAM: serverOpening', event);
            });

            mongoClient.on('serverClosed', function (event) {
                logger.warn('MongoDB SDAM: serverClosed', event);
            });

            mongoClient.on('topologyOpening', function (event) {
                logger.warn('MongoDB SDAM: topologyOpening', event);
            });

            mongoClient.on('topologyClosed', function (event) {
                logger.warn('MongoDB SDAM: topologyClosed', event);
            });

            mongoClient.on('topologyDescriptionChanged', function (event) {
                logger.warn('MongoDB SDAM received topologyDescriptionChanged:', event);
            });

            mongoClient.connect(function (err, mongoClient) {

                if (err) {
                    //return console.log(`Error connecting: ${err}`);
                    throw err;
                }
                const dbase = mongoClient.db('hello_node');


                const port = process.env.PORT || 8080;
                app.listen(port, () => {
                    logger.warn('listening on ' + port);
                    logger.warn('Startup version:', { 'version': TestVersion });

                });


                app.get('/findOneAndReplace', (req, res, next) => {

                    const firstName = faker.name.firstName();
                    const lastName = faker.name.lastName();
                    const id = faker.random.number(5000000);
                    let name = {
                        "firstName": firstName,
                        "lastName": lastName,
                        "connection_string": config.mongo.uri,
                        "version": TestVersion,
                        email: `${firstName}.${lastName}@${faker.internet.domainName().toLowerCase()}`.toLowerCase(),
                        uid: faker.random.uuid(),
                        createDate: faker.date.past(),
                        lastLogin: faker.date.recent()
                    };
                    logger.log('debug', 'Logged to dogstatsd')
                    dbase.collection("name").findOneAndReplace({ _id: id }, name, { upsert: true }, (err, result) => {
                        if (err) {
                            logger.error("Error on findAndReplace", err);
                        }
                        name._id = id;
                        logger.info("findAndReplace", result)
                        res.send({ result: result, doc: name });
                    });

                });

                app.get('/', (req, res, next) => {
                    res.send("OK");
                });

                app.get('/name', (req, res, next) => {
                    dbase.collection('name').find().sort({ _id: -1 }).limit(10).toArray((err, results) => {
                        res.send(results);
                    });
                });

                app.get('/name/:id', (req, res, next) => {
                    if (err) {
                        logger.error(err);
                        throw err;
                    }

                    let id = ObjectID(req.params.id);
                    dbase.collection('name').find(id).toArray((err, result) => {
                        if (err) {
                            logger.error(err);
                            throw err;
                        }

                        res.send(result);
                    });
                });

                app.put('/name/update/:id', (req, res, next) => {
                    const id = {
                        _id: new ObjectID(req.params.id)
                    };

                    dbase.collection("name").updateOne(id, { $set: { first_name: req.body.first_name, last_name: req.body.last_name } }, (err, result) => {
                        if (err) {
                            logger.error(err);
                            throw err;
                        }

                        res.send('user updated sucessfully');
                    });
                });


                app.delete('/name/delete/:id', (req, res, next) => {
                    let id = ObjectID(req.params.id);

                    dbase.collection('name').deleteOne({ _id: id }, (err, result) => {
                        if (err) {
                            logger.error(err);
                            throw err;
                        }

                        res.send('user deleted');
                    });
                });

            })
        });

});
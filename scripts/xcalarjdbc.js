var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');

class XcalarJdbc {
    execsql(config, conn, sql, callback, result) {
        conn.createStatement(function(err, statement) {
            if (err) {
                callback(err);
            } else {
                statement.setFetchSize(config.fetchSize, function(err) {
                    if (err) {
                        callback(err);
                    } else {
                        statement.executeQuery(sql, function(err, resultset) {
                            if (err) {
                                callback(err)
                            } else {
                                callback(null, resultset, result); // everything went well
                            }
                        });
                    }
               });
            }
        })
    }

    execquery(config, sql, callback, result) {
        if (!jinst.isJvmCreated()) {
            jinst.addOption("-Xrs");
            jinst.setupClasspath([config.driver]);
        }
        var jdbc = new JDBC(config);
        jdbc.initialize(function(err) {
            if (err) {
                callback(err)
            }
        });

        jdbc.reserve(function(err, connObj, execsql) {
            if (connObj) {
                var conn = connObj.conn;
                new XcalarJdbc().execsql(config, conn, sql, callback, result);
            }
        });
    }
}

// client code - you can process this the way you want
function usercallback(err, resultset, result) {
    if (err) {
        console.log(err.message)
    } else {
        resultset.toObjArray(function(err, results) {
            if (results.length > 0) {
                console.log("Record count: " + results.length);
                for (result of results) {
                    console.log(result)
                }
            }
        })
    }
}


opt = require('node-getopt').create([
  ['H', 'host=ARG', 'host name', 'localhost'],
  ['P', 'port=ARG', 'port number', 10000],
  ['u', 'user=ARG', 'user name', 'admin'],
  ['p', 'passwd=ARG', 'password', 'admin'],
  ['d', 'driver=ARG', 'driver jar file', process.env.XLRDIR + '/src/sqldf/sbt/target/xcalar-sqldf.jar'],
  ['D', 'driverName=ARG', 'driver name', 'org.apache.hive.jdbc.HiveDriver'],
  ['s', 'sql=ARG', 'sql query', 'show tables'],
  ['m', 'minPoolSize=ARG', 'min pool size', 1],
  ['M', 'maxPoolSize=ARG', 'max pool size', 100],
  ['f', 'fetchSize=ARG', 'fetch size', 100],
  ['h' , 'help'                , 'display this help'],
  ['v' , 'version'             , 'show version']
])              // create Getopt instance
.bindHelp()     // bind option 'help' to default action
.parseSystem(); // parse command line

//console.info({argv: opt.argv, options: opt.options});

var config = {
    // SparkSQL configuration to your server
    url: "jdbc:hive2://" + opt.options.host + ":" + opt.options.port + "/",
    drivername: opt.options.driverName,
    minpoolsize: opt.options.minPoolSize,
    maxpoolsize: opt.options.maxPoolSize,
    user : opt.options.user,
    password: opt.options.passwd,
    properties: {},
    driver: opt.options.driver,
    fetchSize: opt.options.fetchSize
};

console.log(JSON.stringify(config))
xcalarjdbc = new XcalarJdbc()
result = {done : false}
xcalarjdbc.execquery(config, opt.options.sql, usercallback, result)

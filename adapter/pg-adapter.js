var pg = require('pg');

// create a config to configure both pooling behavior
// and client options
// note: all config is optional and the environment variables
// will be read if the config is not present
var config = {
    //host:"192.168.33.10",
  user: 'postgres', //env var: PGUSER
  database: 'cloudappbox', //env var: PGDATABASE
  password: 'postgres', //env var: PGPASSWORD
  port: 5433, //env var: PGPORT
  max: 10, // max number of clients in the pool
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
};

//fix date format with timezone
var types = pg.types;
types.setTypeParser(1114, function(stringValue) {
    return stringValue;
});

types.setTypeParser(1082, function(stringValue) {
    return stringValue;
});


//this initializes a connection pool
//it will keep idle connections open for a 30 seconds
//and set a limit of maximum 10 idle clients
//var pool = new pg.Pool(config);

// to run a query we can acquire a client from the pool,
// run a query on the client, and then return the client to the pool

/*
pool.on('error', function (err, client) {
  // if an error is encountered by a client while it sits idle in the pool
  // the pool itself will emit an error event with both the error and
  // the client which emitted the original error
  // this is a rare occurrence but can happen if there is a network partition
  // between your application and the database, the database restarts, etc.
  // and so you might want to handle it and at least log it out
  console.error('idle client error', err.message, err.stack)
})
*/

function initDBConnection(app) {
    var date  = new Date();
    var ts = date.getMilliseconds();

    var pool = new pg.Pool(config);


    pool.on('error', function (err, client) {
      console.error('idle client error', err.message, err.stack)
    });

    return pool;
}
function connectDB(app, conn) {
    if(conn) {

        return conn;
    }
    var pool = new pg.Pool(config);


    pool.on('error', function (err, client) {
      console.error('idle client error', err.message, err.stack)
    });

    return pool;
    //var client = new pg.Client(config);
    //return client;
}

function doSql(pool, sql, callback) {
    var rollback = function(client, done, callback) {
      client.query('ROLLBACK', function(err) {
        //if there was a problem rolling back the query
        //something is seriously messed up.  Return the error
        //to the done function to close & remove this client from
        //the pool.  If you leave a client in the pool with an unaborted
        //transaction weird, hard to diagnose problems might happen.

        callback();
        return done(err);
      });
    };

    pool.connect(function(err, client, done) {
        if(err) throw err;
        client.query('BEGIN', function(err) {
            if(err) return rollback(client, done, function(){if (callback) callback(err)});
            //as long as we do not call the `done` callback we can do
            //whatever we want...the client is ours until we call `done`
            //on the flip side, if you do call `done` before either COMMIT or ROLLBACK
            //what you are doing is returning a client back to the pool while it
            //is in the middle of a transaction.
            //Returning a client while its in the middle of a transaction
            //will lead to weird & hard to diagnose errors.
            process.nextTick(function() {
                client.query(sql, [], function(error, results) {
                    if (error) {
                        return rollback(client, done, function(){if (callback) callback(error)});
                    } else {
                        client.query('COMMIT', function(){
                            callback(results);
                            done();
                        });

                    }

                });
            });
      });
    });
}

function runSql(pool, sqls, callback, errorcallback) {

    var i = 0;

    var rollback = function(client, done, callback) {
      client.query('ROLLBACK', function(err) {
        //if there was a problem rolling back the query
        //something is seriously messed up.  Return the error
        //to the done function to close & remove this client from
        //the pool.  If you leave a client in the pool with an unaborted
        //transaction weird, hard to diagnose problems might happen.
        callback();
        return done(err);
      });
    };



    pool.connect(function(err, client, done) {
        if(err) throw err;
        client.query('BEGIN', function(err) {
            if(err) return rollback(client, done, function(){if (errorcallback) errorcallback(err)});
            //as long as we do not call the `done` callback we can do
            //whatever we want...the client is ours until we call `done`
            //on the flip side, if you do call `done` before either COMMIT or ROLLBACK
            //what you are doing is returning a client back to the pool while it
            //is in the middle of a transaction.
            //Returning a client while its in the middle of a transaction
            //will lead to weird & hard to diagnose errors.
            process.nextTick(function() {
                function exeSql(){
                    if (i < sqls.length) {

                            if (i == sqls.length - 1) {
                                //console.log(sqls[i]);
                                client.query(sqls[i], [], function(error, results) {
                                    if (error) {
                                        console.log("Error:"+sqls[i]);
                                        return rollback(client, done, function(){if (errorcallback) errorcallback(error)} );
                                    } else {
                                        client.query('COMMIT', function(){
                                            callback();
                                            done();
                                        });

                                    }

                                });
                            } else {
                                //var param = sqls[i].params ? sqls[i].params() : [];
                                 //console.log(sqls[i]);
                                client.query(sqls[i], [], function(error, results) {
                                    if (error) {
                                        console.log("Error:"+sqls[i]);
                                        return rollback(client, done, function(){if (errorcallback) errorcallback(error)});
                                    } else {
                                        i++;
                                        exeSql();
                                    }

                                });
                            }

                    }
                }
                exeSql();
            });
      });
    });
}
function executeSql(pool, sqlArg, out, callback, errorcallback) {

    var sqls = sqlArg;
    var i = 0;

    var rollback = function(client, done, callback) {
      client.query('ROLLBACK', function(err) {
        //if there was a problem rolling back the query
        //something is seriously messed up.  Return the error
        //to the done function to close & remove this client from
        //the pool.  If you leave a client in the pool with an unaborted
        //transaction weird, hard to diagnose problems might happen.
        callback();
        return done(err);
      });
    };

    pool.connect(function(err, client, done) {

        if(err) throw err;
        process.nextTick(function() {
        client.query('BEGIN', function(err) {
            if(err) return rollback(client, done, function(){if (errorcallback) errorcallback(err)});
            //as long as we do not call the `done` callback we can do
            //whatever we want...the client is ours until we call `done`
            //on the flip side, if you do call `done` before either COMMIT or ROLLBACK
            //what you are doing is returning a client back to the pool while it
            //is in the middle of a transaction.
            //Returning a client while its in the middle of a transaction
            //will lead to weird & hard to diagnose errors.

                function executeSql(){
                    try{
                    if (i < sqls.length) {

                            if (i == sqls.length - 1) {
                                var param = sqls[i].params ? sqls[i].params() : [];
                                //console.log("SQL "+i+":"+sqls[i].sql );//+ " " + JSON.stringify(param));
                                client.query(sqls[i].sql, param, function(error, res) {
                                    //console.log(sqls[i].sql + " " + JSON.stringify(param));
                                    if (error) {
                                        console.log("Error:"+"SQL "+i+":"+sqls[i].sql + " " + JSON.stringify(param));
                                        console.log(error);
                                        //process.exit();
                                        rollback(client, done, function(){if (errorcallback) errorcallback(error)});

                                    } else {

                                        var resultArray = [];
                                        var columnNames = [];
                                        if(res) {
                                            for(var j=0;j<res.rows.length;j++) {
                                                var row = {};
                                                for(var key in res.rows[j]) {
                                                    var upperKey = key.toUpperCase();
                                                    columnNames.push(upperKey);
                                                    if(typeof(res.rows[j][key])=='number' && isNaN(res.rows[j][key])) {
                                                        continue;
                                                    }
                                                    row[upperKey] = res.rows[j][key];
                                                }
                                                resultArray.push(row);
                                            }
                                        }
                                        //console.log(JSON.stringify(resultArray));
                                        var results = {
                                            rows: {
                                              item:function(idx) {
                                                return resultArray[idx];
                                              },
                                              length:resultArray.length,

                                            },
                                            columns:function() {
                                              return columnNames || [];
                                            },
                                            insertIdFunc:function() {
                                                //console.log(res.rows);
                                                for(var key in res.rows[0]) {
                                                    return res.rows[0][key]
                                                }
                                              }
                                          };

                                        if (sqls[i].result) {
                                            try {
                                                sqls[i].result(error, results);
                                            } catch (e) {
                                                //console.log(e);

                                                return rollback(client, done,  function(){if (errorcallback) errorcallback(e);});
                                            }
                                        }

                                         client.query('COMMIT', function(){
                                            callback(out, results, pool);
                                            done();
                                         });

                                    }

                                });
                            } else {
                                var param = sqls[i].params ? sqls[i].params() : [];
                                //console.log(sqls[i].sql);// + " " + JSON.stringify(param));
                                client.query(sqls[i].sql, param, function(error, res) {
                                    //console.log("SQL "+i+":"+sqls[i].sql + " " + JSON.stringify(param));
                                    if (error) {
                                        console.log("Error:"+"SQL "+i+":"+sqls[i].sql + " " + JSON.stringify(param));
                                        console.log(error);
                                        //process.exit();
                                        rollback(client, done, function(){if (errorcallback) errorcallback(error);});
                                        //errorcallback(error);

                                    } else {

                                        var resultArray = [];
                                        var columnNames = [];
                                        if(res) {
                                            for(var j=0;j<res.rows.length;j++) {
                                                var row = {};
                                                for(var key in res.rows[j]) {
                                                    var upperKey = key.toUpperCase();
                                                    columnNames.push(upperKey);
                                                    if(typeof(res.rows[j][key])=='number' && isNaN(res.rows[j][key])) {
                                                        continue;
                                                    }
                                                    row[upperKey] = res.rows[j][key];
                                                }
                                                resultArray.push(row);
                                            }
                                        }
                                        //console.log(resultArray);
                                        var results = {
                                            rows: {
                                              item:function(idx) {
                                                return resultArray[idx];
                                              },
                                              length:resultArray.length,

                                            },
                                            columns:function() {
                                              return columnNames || [];
                                            },
                                            insertIdFunc:function() {
                                                //console.log(res.rows);
                                                for(var key in res.rows[0]) {
                                                    return res.rows[0][key]
                                                }

                                              }
                                          };

                                        if (sqls[i].result) {
                                            try {
                                                sqls[i].result(error, results);
                                            } catch (e) {
                                                //console.log(e);
                                                //if (errorcallback) errorcallback(e);
                                                i = sqls.length; //end execution
                                                return rollback(client, done, function(){if (errorcallback) errorcallback(e);});
                                            }

                                        }
                                        i++;
                                        executeSql();
                                    }

                                });
                            }

                    }

                } catch(e) {

                    done(e);
                }
                }
                executeSql();
            });
      });
    });

}


module.exports.connect = connectDB;
module.exports.runSql = runSql;
module.exports.execSql = executeSql;

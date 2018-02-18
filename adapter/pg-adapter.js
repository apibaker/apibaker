var pg = require('pg');
const CP_UTIL_Map2json = require('./map2json.js')


var types = pg.types;
types.setTypeParser(1114, function (stringValue) {
    return stringValue;
});

types.setTypeParser(1082, function (stringValue) {
    return stringValue;
});

var DEBUG = false;

function debugLog(msg) {
    if (DEBUG)
        console.log(msg)
}

function errorLog(msg) {
    console.log(msg)
}

function connectDB(app, config, conn) {
    if (conn) {

        return conn;
    }
    var pool = new pg.Pool(config);


    pool.on('error', function (err, client) {
        errorLog('idle client error', err.message, err.stack)
    });

    return pool;

}

function disconnectDB(app, pool) {

    pool.end();

}

function doSql(pool, sql, callback, errorcallback) {
    var rollback = function (client, done, callback) {
        client.query('ROLLBACK', function (err) {
            callback();
            return done(err);
        });
    };

    var errHandle = function (err) {
        if (err) {
            errorLog(err);
            errorcallback(err)
        }
    }
    

    pool.connect(function (err, client, done) {
        errHandle(err)
        client.query('BEGIN', function (err) {
            if (err) {
                rollback()
                errHandle(err);
                return
            }

            process.nextTick(function () {
                client.query(sql, [], function (error, results) {
                    if (error) {
                        rollback()
                        errHandle(error);
                        return
                    } else {
                        client.query('COMMIT', function () {
                            callback({}, results, pool);
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

    
    var errHandle = function (err) {
        if (err) {
            errorLog(err);
            errorcallback(err)
        }
    }


    pool.connect(function (err, client, done) {
        var rollback = function () {
            client.query('ROLLBACK', function (err) {
                callback();
                return done(err);
            });
        };

        errHandle(err)
        client.query('BEGIN', function (err) {
            if (err) {
                rollback()
                errHandle(err);
                return
            }

            process.nextTick(function () {
                function exeSql() {
                    if (i < sqls.length) {

                        if (i == sqls.length - 1) {
                            client.query(sqls[i], [], function (error, results) {
                                if (error) {
                                    rollback()
                                    errHandle(err);
                                    return
                                } else {
                                    client.query('COMMIT', function () {
                                        callback();
                                        done();
                                    });

                                }

                            });
                        } else {
                            //var param = sqls[i].params ? sqls[i].params() : [];
                            //  console.log(sqls[i]);
                            client.query(sqls[i], [], function (error, results) {
                                if (error) {
                                    rollback()
                                    errHandle(err);
                                    return
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



    var errHandle = function (err) {
        if (err) {
            errorLog(err);
            errorcallback(err)
        }
    }


    pool.connect(function (err, client, done) {

        var rollback = function () {
            client.query('ROLLBACK', function (err) {
                callback();
                return done(err);
            });
        };


        errHandle(err)
        process.nextTick(function () {
            client.query('BEGIN', function (err) {
                if (err) {
                    rollback()
                    errHandle(err);
                    return
                }

                function executeSql() {
                    try {
                        if (i < sqls.length) {

                            if (i == sqls.length - 1) {
                                var param = sqls[i].params ? sqls[i].params() : [];
                                debugLog("SQL " + i + ":" + sqls[i].sql);
                                client.query(sqls[i].sql, param, function (error, res) {
                                    debugLog(sqls[i].sql + " " + JSON.stringify(param));
                                    if (error) {
                                        rollback()
                                        errHandle(err)
                                        return

                                    } else {

                                        var resultArray = [];
                                        var columnNames = [];
                                        if (res) {
                                            for (var j = 0; j < res.rows.length; j++) {
                                                var row = {};
                                                for (var key in res.rows[j]) {
                                                    var upperKey = key.toUpperCase();
                                                    columnNames.push(upperKey);
                                                    if (typeof (res.rows[j][key]) == 'number' && isNaN(res.rows[j][key])) {
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
                                                item: function (idx) {
                                                    return resultArray[idx];
                                                },
                                                length: resultArray.length,

                                            },
                                            columns: function () {
                                                return columnNames || [];
                                            },
                                            insertIdFunc: function () {
                                                for (var key in res.rows[0]) {
                                                    return eval(res.rows[0][key])
                                                }
                                            }
                                        };

                                        if (sqls[i].result) {
                                            try {
                                                sqls[i].result(error, results);
                                            } catch (e) {
                                                
                                                rollback()
                                                errHandle(e)
                                                return
                                            }
                                        }

                                        client.query('COMMIT', function () {
                                            var outObj = {};
                                            outObj = CP_UTIL_Map2json(out, outObj)
                                            callback(outObj, results, pool);
                                            done();
                                        });

                                    }

                                });
                            } else {
                                var param = sqls[i].params ? sqls[i].params() : [];
                                client.query(sqls[i].sql, param, function (error, res) {
                                    if (error) {
                                        debugLog("Error:" + "SQL " + i + ":" + sqls[i].sql + " " + JSON.stringify(param));
                                        rollback()
                                        errHandle(err)
                                        return


                                    } else {

                                        var resultArray = [];
                                        var columnNames = [];
                                        if (res) {
                                            for (var j = 0; j < res.rows.length; j++) {
                                                var row = {};
                                                for (var key in res.rows[j]) {
                                                    var upperKey = key.toUpperCase();
                                                    columnNames.push(upperKey);
                                                    if (typeof (res.rows[j][key]) == 'number' && isNaN(res.rows[j][key])) {
                                                        continue;
                                                    }
                                                    row[upperKey] = res.rows[j][key];
                                                }
                                                resultArray.push(row);
                                            }
                                        }

                                        var results = {
                                            rows: {
                                                item: function (idx) {
                                                    return resultArray[idx];
                                                },
                                                length: resultArray.length,

                                            },
                                            columns: function () {
                                                return columnNames || [];
                                            },
                                            insertIdFunc: function () {
                                                
                                                for (var key in res.rows[0]) {
                                                    return eval(res.rows[0][key])
                                                }

                                            }
                                        };

                                        if (sqls[i].result) {
                                            try {
                                                sqls[i].result(error, results);
                                            } catch (e) {
                                                
                                                i = sqls.length; //end execution
                                                rollback()
                                                errHandle(e)
                                               
                                            }

                                        }
                                        i++;
                                        executeSql();
                                    }

                                });
                            }

                        }

                    } catch (e) {
                        errorLog(e);
                        done(e);

                    }
                }
                executeSql();
            });
        });
    });

}


module.exports.connect = connectDB;
module.exports.disconnect = disconnectDB
module.exports.runSql = runSql;
module.exports.execSql = executeSql;
module.exports.name = "postgre"

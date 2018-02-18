//mysqlAdapter.js
const mysql = require("mysql");
const CP_UTIL_Map2json = require('./map2json.js');

var DEBUG = false;

function debugLog(msg) {
    if (DEBUG)
        console.log(msg)
}

function errorLog(msg) {
    console.log(msg)
}


function connectDB(app, config, conn) {
    if(conn) {
        return conn;
    }
    var pool  = mysql.createPool(config.connectionString);
    //console.log("connection pool created!")
    return pool;
}

function disconnectDB(app, pool) {
    
    pool.end(function (err) {
        if(err)
            errorLog(err)
        else
            debugLog("connections released!")    
    });
}
function runSql(pool, sqls, callback, errorcallback) {

    var i = 0;
    pool.getConnection(function(err, db) {
        if(err) {
            errorLog(err)
        }
        function executeSql() {
        
            if (i < sqls.length) {
                db.beginTransaction(function(err) {
                    if (i == sqls.length - 1) {
                        // var param = sqls[i].params ? sqls[i].params() : [];
                        // console.log(sqls[i]);
                        db.query(sqls[i], [], function(error, results) {
                            if (error) {
                                errorLog("Error Occurred. " + error);
                                db.rollback(function() {
                                    db.release();
                                    errorcallback(error);
                                });
                            } else {
                                

                                db.commit(function(err) {
                                    if (err) {
                                        db.rollback(function() {
                                            
                                        });
                                    }
                                    i++;
                                    callback({}, results);
                                    db.release();
                                    //console.log("db released!")
                                });
                            }
                            
                        });
                    } else {
                        // var param = sqls[i].params ? sqls[i].params() : [];
                        // console.log(sqls[i] );
                        db.query(sqls[i], [], function(error, results) {
                            if (error) {
                                errorLog("Error Occurred. " + error);
                                i = sqls.length; //end execution
                                db.rollback(function() {
                                    errorcallback(error);
                                });

                            } else {
                                i++;
                                executeSql();
                            }
                            
                        });
                    }
                });
            }
        
        };
        executeSql();
    });
}
function executeSql(pool, sqlArg, out, callback, errorcallback) {

    var sqls = sqlArg;
    var i = 0;
    pool.getConnection(function(err, db) {
        function executeSql() {
        
            if (i < sqls.length) {
                db.beginTransaction(function(err) {
                    if (i == sqls.length - 1) {
                        var param = sqls[i].params ? sqls[i].params() : [];
                        db.query(sqls[i].sql, param, function(error, resultArray) {
                            if (error) {
                                errorLog("Error Occurred. " + error);
                                db.rollback(function() {
                                    errorcallback(error);
                                    
                                });
                                db.release();
                            } else {
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
                                        return resultArray.insertId;
                                      }
                                  };

                                if (sqls[i].result) {
                                    try {
                                        sqls[i].result(error, results);
                                    } catch (e) {
                                        errorLog(e)
                                        errorcallback(e);
                                    }
                                }
                                
                                db.commit(function(err) {
                                    if (err) {
                                        db.rollback(function() {
                                            
                                        });
                                    }
                                    i++;
                                    var outObj = {};
                                    outObj = CP_UTIL_Map2json(out, outObj)
                                    callback(outObj, results);
                                    db.release();
                                    
                                });
                            }
                            
                        });
                    } else {
                        var param = sqls[i].params ? sqls[i].params() : [];
                        db.query(sqls[i].sql, param, function(error, resultArray) {
                            if (error) {
                                errorLog("Error Occurred. " + error);
                                i = sqls.length; //end execution
                                db.rollback(function() {
                                    errorcallback(error);
                                });

                            } else {
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
                                        return resultArray.insertId;
                                      }
                                  };

                                if (sqls[i].result) {
                                    try {
                                        sqls[i].result(error, results);

                                    } catch (e) {
                                        i = sqls.length; //end execution
                                        errorcallback(e);
                                    }

                                }
                                i++;
                                executeSql();
                            }
                            
                        });
                    }
                });
            }
        
        };
        executeSql();
    });

}


module.exports.connect = connectDB;
module.exports.runSql = runSql;
module.exports.execSql = executeSql;
module.exports.name = "mysql"
module.exports.disconnect = disconnectDB
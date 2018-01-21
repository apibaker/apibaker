//mysqlAdapter.js
const mysql = require("mysql");
const CP_UTIL_Map2json = require('./map2json.js');

function connectDB(app, config, conn) {
    if(conn) {
        return conn;
    }
    var pool  = mysql.createPool(config);
    //console.log("connection pool created!")
    return pool;
}

function disconnectDB(app, pool) {
    pool.end(function (err) {
        if(err)
        console.error(err)
    });
}
function runSql(pool, sqls, callback, errorcallback) {

    var i = 0;
    pool.getConnection(function(err, db) {
        if(err) {
            console.error(err)
        }
        function executeSql() {
        
            if (i < sqls.length) {
                db.beginTransaction(function(err) {
                    if (i == sqls.length - 1) {
                        // var param = sqls[i].params ? sqls[i].params() : [];
                        // console.log(sqls[i]);
                        db.query(sqls[i], [], function(error, results) {
                            if (error) {
                                db.rollback(function() {
                                    if (errorcallback) {
                                        errorcallback(error);
                                    } else {
                                        console.log("Error Occurred. " + error);
                                    }
                                    db.release();

                                });
                            } else {
                                

                                db.commit(function(err) {
                                    if (err) {
                                        db.rollback(function() {
                                            throw err;
                                        });
                                    }
                                    i++;
                                    if (callback) 
                                        callback();
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
                                db.rollback(function() {
                                    if (errorcallback) {
                                        errorcallback(error);
                                    } else {
                                        console.log("Error Occurred. " + error);
                                    }
                                    i = sqls.length; //end execution
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
                        // console.log(sqls[i].sql + " " + JSON.stringify(param));
                        db.query(sqls[i].sql, param, function(error, resultArray) {
                            if (error) {
                                db.rollback(function() {
                                    if (errorcallback) {
                                        errorcallback(error);
                                    } else {
                                        // console.log("Error Occurred. " + error);
                                    }
                                    console.log("Error Occurred. " + error);
                                    db.release();
                                    //console.log("db released!")

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
                                        if (errorcallback) errorcallback(e);
                                    }
                                }
                                
                                db.commit(function(err) {
                                    if (err) {
                                        db.rollback(function() {
                                            throw err;
                                        });
                                    }
                                    i++;
                                    var outObj = {};
                                    outObj = CP_UTIL_Map2json(out, outObj)
                                    if (callback) 
                                        callback(outObj, error, results);
                                    db.release();
                                    //console.log("db released!")
                                });
                            }
                            
                        });
                    } else {
                        var param = sqls[i].params ? sqls[i].params() : [];
                        // console.log(sqls[i].sql + " " + JSON.stringify(param));
                        db.query(sqls[i].sql, param, function(error, resultArray) {
                            if (error) {
                                db.rollback(function() {
                                    if (errorcallback) {
                                        errorcallback(error);
                                    } else {
                                        // console.log("Error Occurred. " + error);
                                    }
                                    console.log("Error Occurred. " + error);
                                    i = sqls.length; //end execution
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
                                        if (errorcallback) errorcallback(e);
                                        i = sqls.length; //end execution
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
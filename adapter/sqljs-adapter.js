const sql = require('sql.js');
const CP_UTIL_Map2json = require('./map2json.js')
const container = {};
const DEBUG = false;
const debug = function(msg) {
    if(DEBUG) {
        console.log(msg)
    }
}

module.exports.name = "sqljs"

module.exports.connect = function(app) {
    container.CP_SQLJS_Schema = container.CP_SQLJS_Schema || {};
    container.CP_SQLJS_Schema[app] = container.CP_SQLJS_Schema[app] || new sql.Database();
    return container.CP_SQLJS_Schema[app];
}

module.exports.disconnect = function(app) {

}


module.exports.runSql = function(db, sqls, succ, err) {
    try {
        for (let i = 0; i < sqls.length; i++) {
            //console.log(sqls[i])
            db.run(sqls[i]);
        }
        succ(true)
    } catch (e) {
        err(e)
    }
}

module.exports.execSql = function(db, sqlArg, out, callback, errorcallback) {

    var sqls = sqlArg;
    if (sqls.length == 0) {
        callback();
    }

    for (let i = 0; i < sqls.length; i++) {
        var stmt = null;
        try {
            debug(sqls[i].sql)
            let param = sqls[i].params ? sqls[i].params() : [];
            stmt = db.prepare(sqls[i].sql, param);
            let resultArray = [];
            let columnNames = [];
            while (stmt.step()) { //
                let row = stmt.getAsObject();
                columnNames = stmt.getColumnNames();
                resultArray.push(row);
            }
            var results = {
                rows: {
                    item: function(idx) {
                        return resultArray[idx];
                    },
                    length: resultArray.length,

                },
                columns: function() {
                    return columnNames || [];
                },
                insertIdFunc: function() {
                    let idStmt = db.prepare("SELECT last_insert_rowid()", []);
                    if (idStmt.step()) {
                        let id = idStmt.getAsObject();
                        return id["last_insert_rowid()"];
                    }
                }
            };
            if (sqls[i].result) {
                try {
                    sqls[i].result(db, results);
                } catch (e) {
                    debug("error 2:" + e);
                    errorcallback(e);
                    return;
                }
            }
            if (i == sqls.length - 1) {
                if (callback) {
                    var outObj = {};
                    outObj = CP_UTIL_Map2json(out, outObj)
                    callback(outObj, results, db);
                }
                stmt.free();
                return;
            }


        } catch (e) {
            debug("error 1:" + e);
            if (stmt != null)
                stmt.free();
            errorcallback(e);
            break;
        }

    }
}

const REQ_TYPE_MAP = {
    "Create":"POST",
    "AddReference":"POST",
    "Delete":"POST",
    "RemoveReference":"POST",
    "Read":"GET",
    "Count":"POST",
    "Exist":"POST",
    "Query":"POST",
    "QueryRef":"POST",
    "CountRef":"POST",
    "ExistRef":"POST",
    "QueryRefCandidate":"POST",
    "CountRefCandidate":"POST",
    "ExistRefCandidate":"POST",
    "Update":"POST",
    "QueryById":"GET"
}
var prepareInput = function(param, actionType, entName, refeEntName) {
  var input = {};
  input.obj = param.obj || {};
  if(param.from) {
    input.from = param.from;
  }
  if(param.size) {
    input.size = param.size;
  }
  if(actionType==("QueryRef") || actionType==("CountRef") || actionType==("ExistRef")
            || actionType==("QueryRefCandidate") || actionType==("CountRefCandidate")
            || actionType==("ExistRefCandidate")) {
    input.obj[refeEntName+'Id'] = param.id || 0;
  } else if(actionType=="Query" && param.id) {
    input.obj[entName+'Id_1'] = param.id || 0;
  } else {
    input.obj[entName+'Id'] = param.id || 0;
  }
  return input;

}

function CP_UTIL_Json2map(obj, map, prefixKey) {

    map = map || {};
    for(var key in obj) {
      var val = obj[key];
      if(Array.isArray(val)) {
        var arrayList = new Array();
        var length = val.length;
        for(var i=0;i<length;i++) {
          var item = val[i];
          var objMap = {};
          CP_UTIL_Json2map(item, objMap, null);
          arrayList.push(objMap);
          map[prefixKey!=null?prefixKey+key:key] = arrayList;
        }
      } else if(typeof(val) === 'object') {
        CP_UTIL_Json2map(val, map, prefixKey!=null?prefixKey+key+",":key+",");
      } else {
        map[prefixKey!=null?prefixKey+key:key] = obj[key];
      }

    }
    return map;


}



var execAction = function(input, DBCmd, succ, err, dbAdapter, conn) {


  var inMap=CP_UTIL_Json2map(input.obj);//input.obj;
  var out={};
  var sqlArg = new Array();
  for(var i=0;i<DBCmd.length;i++) {
    var DBCmdObj = DBCmd[i];
    var cmd = DBCmdObj.cmd;
    if(!cmd) {
      continue;
    }
    var results = DBCmdObj.outResult;
    var params = DBCmdObj.inParam;
    if(DBCmdObj.inRefKeys && DBCmdObj.inMultiple) {

      var objs = inMap[DBCmdObj.inRefKeys];
      if(objs) {
        var size = objs.length;
        for(var j=0;j<size;j++) {
          var sqlparamfunc = null;
          if(DBCmdObj.inParam) {
            sqlparamfunc = (function(obj, params, isRefe) {
              return function() {

                var parray = [];
                for(var k=0;k<params.length;k++) {
                  if(params[k]==-1) {
                    parray.push(null);
                  } else if(isRefe[k]) {
                    parray.push(setSQLParam([params[k]], obj)[k]);
                  } else if(!isRefe[k]) {
                    parray.push(setSQLParam([params[k]], inMap)[k]);
                  }
                }
                return parray;
              }
            })(objs[j], params, DBCmdObj.inParamRefe);
          }
          var resultfunc = null;
          if(DBCmdObj.outResult) {
            resultfunc = function(tx, result) {};
          }
          sqlArg.push({sql:cmd,params:sqlparamfunc, result:resultfunc});
        }
      }
    }
    else if(DBCmdObj.checkExist && !inMap[DBCmdObj.checkExist]) {
      // console.log("exist: "+inMap)
      // console.log("exist: "+DBCmdObj.checkExist+" "+inMap[DBCmdObj.checkExist]);
      continue;
    } else {
      var sqlparamfunc = (function(params){
        return function(){
          return setSQLParam(params, inMap);
        }
      })(params);


      var resultfunc = function(){};
      if(DBCmdObj.outInsertId) {
        resultfunc = (function(results, out){
          return function(tx, result){
            setSQLResult(tx, result, -1, results, out, true, inMap);
          };
        })(results, out);
      } else if(DBCmdObj.outMultiple) {

        if(DBCmdObj.inSize) {
          var sizeVal = input.size?parseInt(input.size):DBCmdObj.inPageSize;
          if(!isNaN(sizeVal)) {
            sizeVal = Math.min(sizeVal, DBCmdObj.inPageSize);
            cmd = cmd.replace(/%SIZE%/, (sizeVal));
            if(sizeVal)
            out.size = sizeVal;
          }
        }

        if(DBCmdObj.inFrom) {


          var fromVal = input.from?parseInt(input.from):0;
          cmd = cmd.replace(/%FROM%/, (fromVal));
          if(!isNaN(fromVal) && typeof(fromVal)!='undefined' && fromVal!=0) {
            out.from = fromVal;
          }

        }



        if(DBCmdObj.outRefName) {
          resultfunc = (function(results, out, refName){
            return function(tx, result) {
              var outList = new Array();
              for (var k = 0; k < result.rows.length; k++) {
                var itemMap = {};
                outList.push(itemMap);
                setSQLResult(tx, result, k, results, itemMap);
              }
              out.refName =  outList;
            }
          })(results, out, DBCmdObj.outRefName);
        } else if(DBCmdObj.outQuery) {
          resultfunc = (function(results, out){
            return function(tx, result) {
              var outList = new Array();
              for (var k = 0; k < result.rows.length; k++) {
                var itemMap = {};
                outList.push(itemMap);
                setSQLResult(tx, result, k, results, itemMap, false, null, true);
              }
              if(outList.length>0) {
                out.data = outList;
              }
            }
          })(results, out);
        } else {
          resultfunc =  (function(results, out){
            return function(tx, result){
              for (var k = 0; k < result.rows.length; k++) {
                setSQLResult(tx, result, k, results, out);
              }
            };
          })(results, out);
        }

      } else if(DBCmdObj.outCount) {
        resultfunc = (function(results, out){
          return function(tx, result) {
            var size = 0;
            if(result.rows.length>0) {
              size = eval(result.rows.item(0).CNT);
            }
            out.count = (size);
            if(size==0) {
              out.data = [];
            }

          }
        })(results, out);
      } else if(DBCmdObj.outCheckInUse) {
        resultfunc = (function(results, out, propName){
          return function(tx, result) {
            if(result.rows.length>0 && result.rows.item(0)[propName]) {

              throw {message:'Data is in use!'};
            }
          }
        })(results, out, DBCmdObj.outCheckPropName);
      }

      sqlArg.push({sql:cmd,params:sqlparamfunc, result:resultfunc});
    }


  }
  dbAdapter.execSql(conn, sqlArg, out, succ, err);

}


function setSQLParam (params, map) {
  
  var sqlparam = new Array();

  for(var i=0;i<params.length;i++) {
    var idx = params[i][0]-1;
    var type = params[i][1];
    var key = params[i][2];
    var opt = params[i][3];
    var cndIdx = null;;
    if(typeof(params[i][4])!="undefined" && eval(params[i][4])!=null && params[i][4]!=-1){
      cndIdx = params[i][4];
      key = key+"_"+cndIdx;
      //key = key+"_"+(idx+1);
      key = key.replace(/\,/g, '_');
    }

    
    var pval = map[key];

    //      if(typeof(pval) == "undefined") {
    //          console.log(key +"="+ pval);
    //      }
    if(key==null && (type==("Id") || type==("Long"))) {
      sqlparam[idx]=null;
      continue;
    } else if(["String","Text"].indexOf(type)==-1) {
 
      if(pval==="") {

        sqlparam[idx] = null;
        continue;
      }

    }
    
    if(opt === 'Start-With') {
      sqlparam[idx]=pval+'%';
    } else if(opt === 'End-With') {
      sqlparam[idx]='%'+pval
    } else if(opt === 'Contains') {
      sqlparam[idx]='%'+pval+'%';
    } else {
      
      sqlparam[idx]=pval;

      if(typeof(sqlparam[idx])=='undefined' || sqlparam[idx]===""){
        sqlparam[idx]=null;
      } else if(["Integer","Long", "Id"].indexOf(type)!=-1) {
        sqlparam[idx]=parseInt(sqlparam[idx]);
      } else if(["Double","Float","Decimal"].indexOf(type)!=-1) {
        sqlparam[idx]=parseFloat(sqlparam[idx]);
      } else if(["Boolean"].indexOf(type)!=-1) {
        if(String(sqlparam[idx])=='true')
        sqlparam[idx]=1;
        else
        sqlparam[idx]=0;
      }

    }
    

  }
  //console.log(sqlparam)
  return sqlparam;


}


function setSQLResult (tx, result, rowidx, results, map, insertId, inMap, query) {

  for(var i=0;i<results.length;i++) {
    var idx = results[i][0];
    var type = results[i][1];
    var key = results[i][2];
    var displayName = results[i][3];
    var displayNameDefined = results[i][4];
    var itemKey = null;
    if(displayNameDefined) {
      itemKey = displayName.toUpperCase();
      key = query?displayName:key;
    } else {
      itemKey = displayName.toUpperCase();
      if(key.indexOf(",")!=-1) {
        var keys = key.split(",");
        itemKey = keys[keys.length-1].toUpperCase();
      }
    }
    if(insertId) {
      map[key] = result.insertIdFunc();//String(result.insertIdFunc());
      inMap[key] = result.insertIdFunc();//String(result.insertIdFunc());
    } else if(type==="Boolean"){
      if(result.rows.item(rowidx)[itemKey]!=null && typeof(result.rows.item(rowidx)[itemKey])!='undefined') {

        var str = "";
        var val = result.rows.item(rowidx)[itemKey];

        if(val==1 || String(val).toUpperCase()=='TRUE') {
          str = (true);
          //map.set(key, String(true));
        } else {
          str = (false);
          //map.set(key, String(false));

        }
        map[key] = str;

      }
      //map.set(key, String(result.rows.item(rowidx)[itemKey]==1 || result.rows.item(rowidx)[itemKey].toUpperCase()=='TRUE'));
    } else if(["Integer","Long", "Id", "Short"].indexOf(type)!=-1) {
      if(result.rows.item(rowidx)[itemKey]!=null && typeof(result.rows.item(rowidx)[itemKey])!='undefined' && result.rows.item(rowidx)[itemKey]!='NaN')
      map[key] = parseInt(result.rows.item(rowidx)[itemKey]);
    } else if(["Double","Float","Decimal"].indexOf(type)!=-1) {
      if(result.rows.item(rowidx)[itemKey]!=null && typeof(result.rows.item(rowidx)[itemKey])!='undefined' && result.rows.item(rowidx)[itemKey]!='NaN')
      map[key] = parseFloat(result.rows.item(rowidx)[itemKey]);
    } else {
      if(result.rows.item(rowidx)[itemKey]!=null && typeof(result.rows.item(rowidx)[itemKey])!='undefined')
      map[key] = String(result.rows.item(rowidx)[itemKey]);
    }
  }

}


function runTest(tests, model, conn, dbAdapter, done, idx = 0, elapse = 0, result = {}) {

  let actionDef = model.ActDef[tests[idx].action];
  let action = actionDef.model;
  let actionName = action.Name;
  let actionType = action.Type;
  let entName = action.Entity;
  var refeEntName = actionDef.refeDef ? actionDef.refeDef.entDef.name : null;
  var input = prepareInput(tests[idx].in, actionType, entName, refeEntName || null);

  var st = (new Date()).getTime();
  result.tests = result.tests || [];
  debuglog(model.Runtime[tests[idx].action]);
  execAction(input, model.Runtime[tests[idx].action], function (res) {
    if (JSON.stringify(tests[idx].out) === JSON.stringify(res)) {
      debuglog(JSON.stringify(res))
      var et = (new Date()).getTime();
      elapse += et - st;
      
      infolog(dbAdapter.name +' '+ model.raw.name+'/'+tests[idx].action+'/'+idx+ ' done! ' + (et-st) / 1000 + ' sec')
      
      result.tests.push({test:model.raw.name, idx:idx, action:tests[idx].action, elapse: (et-st), succ:true, expected: tests[idx].out, actual:res})   
      
      if (idx + 1 < tests.length) {
        runTest(tests, model, conn, dbAdapter, done, idx + 1, elapse, result)
      }
      else {
        infolog(dbAdapter.name +' '+ model.raw.name + ' done! ' + ( elapse ) / 1000 + ' sec')
        result.all = {test:model.raw.name, elapse: elapse, succ:true}
        if(done)
          done(result); 
      }

    } else {
      errlog(model.raw.name + '[' + idx + ']. expected:' + JSON.stringify(tests[idx].out) + '\n but:' + JSON.stringify(res))
      result.tests.push({test:model.raw.name, idx:idx, action:tests[idx].action, elapse: (et-st), succ:false, expected: tests[idx].out, actual:res})   
      result.all = {test:model.raw.name, elapse: elapse, succ:false}
      if(done)
          done(result); 
    }
    
  }, function (err) {
    errlog(err)
  }, dbAdapter, conn);

}


const DEBUG = false;

function debuglog(msg) {
  if (DEBUG) {
    console.log(msg)
  }
}

function infolog(msg) {

  console.log(msg)

}
function errlog(msg) {

  console.error(msg)

}


if(typeof(module)!=="undefined" && typeof(module.exports)!=="undefined") {
  module.exports = {
    execAction:execAction,
    prepareInput:prepareInput,
    runTest:runTest
  }
}

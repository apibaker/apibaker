
function CP_UTIL_Map2json(map, obj) {
    obj = obj || {}
    var keySet = Object.keys(map)
    for(var i=0;i<keySet.length;i++) {
      var key = keySet[i];
      var keys = key.split(",");
      var newObj = obj;
      for(var j=0;j<keys.length-1;j++) {
        var newObj2 = newObj[keys[j]];
        if(newObj2 == null) {
          newObj2 = new Object();
          newObj[keys[j]]=newObj2;
        }
        newObj = newObj2;
      }
      var val = map[key];
      if(Array.isArray(val)) {
        var array = new Array();
        for(var k=0;k<val.length;k++) {
          var newObj3 = new Object();
          CP_UTIL_Map2json(val[k], newObj3);
          array.push(newObj3);
        }
        newObj[keys[keys.length-1]]=array;
      } else if( (typeof val === "object") ) {
        var newObj3 = new Object();
        CP_UTIL_Map2json(val, newObj3);
        newObj[keys[keys.length-1]]=newObj3;
      } else {
        newObj[keys[keys.length-1]]=val;
      }
    }
    return obj;

}

module.exports = CP_UTIL_Map2json;
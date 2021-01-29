var client = require("./Client");
var context = null;

var isNodeJs = false;
// Explicitly check if this code is running under nodejs
if ((typeof process !== 'undefined') &&
    (typeof process.versions !== 'undefined') &&
    (typeof process.versions.node !== 'undefined')) {
    var requireContext = require('require-context');
    context = requireContext(__dirname, false, /_xcrpc\.js$/);
    isNodeJs = true;
} else {
    context = require.context(".", false, /_xcrpc\.js$/);
}

// We want to get all of the top level service files (*_xcrpc.js)
// and gather their exposed services to re-expose here at the top level.
var obj = {};
context.keys().forEach(function (key) {
    // We have the name of the service file, let's get the actual module
    var thisMod = context(key);
    for (var k in thisMod) {
        if (thisMod.hasOwnProperty(k)) {
            // Now we can re-export each of the items exported in the original
            module.exports[k] = thisMod[k];
        }
    }
});

if(isNodeJs) {
    //want to get all service info objects into one object and expose it
    var _serviceInfo = {}
    context = requireContext(__dirname + "/serviceInfo", false, /_serviceInfo\.js$/);
    context.keys().forEach(function (key) {
        var infoMod = context(key);
        for (var k in infoMod) {
            if (infoMod.hasOwnProperty(k) && k === 'serviceInfo') {
                for (var service in infoMod[k]){
                    _serviceInfo[service] = infoMod[k][service];
                }
            }
        }
    });

    exports.ServiceInfo = _serviceInfo
}

// Expose all enum maps to exports.EnumMap.<mapName>
if (isNodeJs) {
    context = requireContext(__dirname + "/enumMap", true, /.json$/);
} else {
    context = require.context("./enumMap", true, /.json$/);
}
const _EnumMap = {};
context.keys().forEach(function(key) {
    const mapName = key.substring(
        key.lastIndexOf('/') + 1,
        key.lastIndexOf('.json')
    );
    _EnumMap[mapName] = context(key);
});
exports.EnumMap = _EnumMap;

exports.XceClient = client.XceClient

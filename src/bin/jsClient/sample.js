/* sample.js
 * This file shows how to use the protobuf-based API layer.
 * This is intended mostly as a demonstration of the APIs, not as an instruction
 * for exactly how to use them.
 *
 * The XceClient object as well as the service objects (xce.Echo) could exist
 * as singletons (global variables) or something similar, because needing
 * to instantiate them on every usage seems annoying.
 */

function successUsage() {
    var client = new xce.XceClient("http://localhost/app/service/xce/");
    var echoService = new xce.EchoService(client);
    var echoRequest = new proto.xcalar.compute.localtypes.Echo.EchoRequest();
    echoRequest.setEcho("hello from the browser!");
    echoService.echoMessage(echoRequest)
    .then(function(echoInfo) {
        console.log("recieved an echo of " + echoInfo.getEchoed());
    })
    .fail(function(error) {
        console.error("recieved failure!");
    });
}

function errorUsage() {
    var client = new xce.XceClient("http://localhost/app/service/xce/");
    var echoService = new xce.EchoService(client);
    var errRequest = new proto.xcalar.compute.localtypes.Echo.EchoErrorRequest();
    errRequest.setError("hello error!");
    echoService.echoErrorMessage(errRequest)
    .then(function(badOutput) {
        console.error("recieved a false positive of " + badOutput);
    })
    .fail(function(error) {
        console.log("recieved expected failure of " + error);
    });
}

// Code snippet taken from https://github.com/ariya/phantomjs/issues/10687 from JamesMGreene
phantom.onError = function(msg, trace) {
    var msgStack = ['PHANTOM ERROR: ' + msg];
    if (trace && trace.length) {
        msgStack.push('TRACE:');
        trace.forEach(function(t) {
            msgStack.push(' -> ' + (t.file || t.sourceURL) + ': ' + t.line + (t.function ? ' (in function ' + t.function + ')' : ''));
        });
    }
    console.error(msgStack.join('\n'));
    phantom.exit(1);
};
testSuiteFn = undefined;
phantom.injectJs('mgmttestactual.js'); // If successfully loaded, this guy will overwrite testSuiteFn
if (testSuiteFn == undefined) {
    console.log("Failed to load mgmttestactual.js")
    phantom.exit(1);
}

testSuiteFn($, {});

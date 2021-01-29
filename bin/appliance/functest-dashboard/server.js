var express = require("express");
var fs = require("fs");
var bodyParser = require("body-parser");
var os = require("os");
var hostname = os.hostname();
var exec = require('child_process').exec;
var app = express();
if ( !fs.existsSync("/tmp/functest") ) {
    fs.mkdirSync("/tmp/functest");
}

var Status = {
    "Error": -1,
    "Unknown": 0,
    "Ok": 1,
    "Done"   : 2,
    "Running": 3,
    "Incomplete": 4
};

app.get("/", function(req, res) {
    res.sendFile(__dirname + "/html/xcInfra.html");
});

app.get("/view", function(req, res) {
    res.sendFile("/tmp/functest/html/index_"+hostname+".html");
});

var server = app.listen(3001, function() {
    console.log("Working on port 3001");
});

server.timeout = 120000000

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json({limit: "50mb"}));

app.all("/*", function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    res.header("Access-Control-Allow-Headers", "Content-Type");
    next();
});

app.post("/test", function(req, res) {
    var execString = "python2.7 " + __dirname + "/startFuncTests.py --target /tmp/functest ";
    console.log(execString);
    var out = exec(execString);

    out.on('close', function(code) {
        // code(1) means files were changed while being archived
        if (code == 0) {
            console.log("Succeeded");
            return res.send({"status": Status.Ok});
        } else {
            console.log("Failed");
            console.log("Error code is " + code);
            res.status(400);
            return res.send({"status": Status.Error, "logs": "Error code is " + code});
        }
    });
});

app.get("/stop", function(req, res) {
    var execString = "sudo service xcalar stop";
    var out = exec(execString);

    out.on('close', function(code) {
        // code(1) means files were changed while being archived
        if (code == 0) {
            console.log("Succeeded");
            return res.send({"status": Status.Ok});
        } else {
            console.log("Failed");
            console.log("Error code is " + code);
            res.status(400);
            return res.send({"status": Status.Error, "logs": "Error code is " + code});
        }
    });
});

app.get("/start", function(req, res) {
    var execString = "sudo service xcalar start";
    var out = exec(execString);

    out.on('close', function(code) {
        // code(1) means files were changed while being archived
        if (code == 0) {
            console.log("Succeeded");
            return res.send({"status": Status.Ok});
        } else {
            console.log("Failed");
            console.log("Error code is " + code);
            res.status(400);
            return res.send({"status": Status.Error, "logs": "Error code is " + code});
        }
    });
});

app.post("/restart", function(req, res) {
    var execString = "sudo service xcalar restart";
    var out = exec(execString);

    out.on('close', function(code) {
        // code(1) means files were changed while being archived
        if (code == 0) {
            console.log("Succeeded");
            return res.send({"status": Status.Ok});
        } else {
            console.log("Failed");
            console.log("Error code is " + code);
            res.status(400);
            return res.send({"status": Status.Error, "logs": "Error code is " + code});
        }
    });
});

app.post("/getParams", function(req, res) {
    var filename = "/etc/xcalar/default.cfg";
    fs.readFile(filename, "utf8", function(err, config) {
        // console.log(config);
        return res.send(config);
    });
});

app.post("/saveParams", function(req, res) {
    var data = req.body.data;
    var filename = req.body.config;
    console.log(req.body);
    fs.writeFile(filename, data, function(err) {
        return res.send({"status": Status.Ok});
    });
});

app.post("/status", function(req, res) {
    var execString = "/sbin/ip route|awk '/default/ { print $3 }'"
    var out = exec(execString, function(error, stdout, stderr) {
        var execString = "/opt/xcalar/bin/xccli -c version --ip " + stdout;
        var out = exec(execString, function (error, stdout, stderr) {
            if (stdout.indexOf("Error: Connection refused") > -1) {
                res.status(400);
                return res.send({"status": Status.Error, "logs": stdout});
            } else {
                return res.send({"status": Status.Ok});
            }
        });
    });
});
// Async extension for XcalarApiService.js
XcalarApiServiceClient.prototype.queueWorkAsync = function(workItem) {
  var self = this;
  return (this.send_queueWorkAsync(workItem)
  .then(function(result) {
    return self.recv_queueWorkAsync.call(self);
  }));
};

var gThriftTimeCheck = false;
var gThriftTracker = {count: 0, numInQueue: 0, numInLongQueue: 0};
XcalarApiServiceClient.prototype.send_queueWorkAsync = function(workItem) {
  var checking = false;
  if (gThriftTimeCheck) {
    checking = true;
    gThriftTracker.numInQueue++;
    gThriftTracker.count++;
    var id = XcalarApisTStr[workItem.api].slice(9) + "#" + gThriftTracker.count;
    var timeExpired = false;
    var startTime = Date.now();
    var timer1 = setTimeout(function() {
        var msg = id + " has taken over 10 seconds. ";
        if (gThriftTracker.numInQueue - 1) {
          msg += (gThriftTracker.numInQueue - 1) + " other items in queue.";
        }
        var item = {};
        for (var i in workItem.input) {
          if (workItem.input[i] !== null) {
            item = workItem.input[i];
            break;
          }
        }
        console.warn(msg, item);
        gThriftTracker.numInLongQueue++;
        timeExpired = true;
    }, 10000);
    var timer2 = setTimeout(function() {
        var msg = id + " has taken over 1 minute! ";
        if (gThriftTracker.numInQueue - 1) {
          msg += (gThriftTracker.numInQueue - 1) + " other items in queue.";
        }
        console.warn(msg);
    }, 60000);
  }

  var onComplete = function() {
    if (checking) {
      clearTimeout(timer1);
      clearTimeout(timer2);
      gThriftTracker.numInQueue--;
      if (timeExpired) {
          var s = Math.ceil((Date.now() - startTime) / 1000);
          gThriftTracker.numInLongQueue--;
          var msg = id + " has finally returned after " + s + " seconds. ";
          if (gThriftTracker.numInLongQueue) {
             msg += gThriftTracker.numInLongQueue +
                    " other items taking a long time.";
          }
          console.warn(msg);
      }
    }
  };
  this.output.writeMessageBegin('queueWork', Thrift.MessageType.CALL, this.seqid);
  var args = new XcalarApiService_queueWork_args();
  args.workItem = workItem;
  args.write(this.output);
  this.output.writeMessageEnd();

  return (this.output.getTransport()
    .jqRequest(null, this.output.getTransport().flush(true), null, onComplete));
};

XcalarApiServiceClient.prototype.recv_queueWorkAsync = function() {
  var deferred = jQuery.Deferred();

  var ret = this.input.readMessageBegin();
  var fname = ret.fname;
  var mtype = ret.mtype;
  var rseqid = ret.rseqid;

  if (mtype == Thrift.MessageType.EXCEPTION) {
    var x = new Thrift.TApplicationException();
    x.read(this.input);
    this.input.readMessageEnd();
    deferred.reject(x);
  } else {
    var result = new XcalarApiService_queueWork_result();
    result.read(this.input);
    this.input.readMessageEnd();

    if (null !== result.err) {
      deferred.reject(result.err);
    } else if (null !== result.success) {
      deferred.resolve(result.success);
    } else {
      deferred.reject('queueWork failed: unknown result');
    }

  }
  return (deferred.promise());
};

# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# This app runs for a lifetime of a cluster. Its purpose is to expose
# performance information pertaining to the parent process (usrnode) over the
# HTTP protocol.

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import time
from urllib.parse import urlparse, parse_qs
from http import HTTPStatus

from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
import xcalar.container.xpu_host as xpu_host

# Start the logger.
import xcalar.container.context as ctx
logger = ctx.get_logger()

_header = """<!DOCTYPE html><html>
<head><title>Status</title>
<style>
    th {
        padding: 8px;
        text-align: left;
        border: 1px solid #ddd;
        background-color: #F1F1F1;
    }
    .headerCell {
        padding-right: 2em;
        padding-left: 2em;
    }
</style>
</head><body>"""


class Handler(BaseHTTPRequestHandler):

    def write(self, data):
        self.wfile.write(data.encode("utf-8"))

    def do_GET(self):
        query_components = parse_qs(urlparse(self.path).query)
        json_out = query_components.get("json", False)

        self.send_response(HTTPStatus.OK)
        if json_out:
            self.send_header("Content-type", "application/json")
            self.end_headers()
        else:
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.write(_header)

        try:
            histo_proto = xpu_host.fetch_parent_perf_info()
            logger.info("Got " + str(len(histo_proto.histograms)) +
                        " histograms from the parent")
            self.print_proto(histo_proto, json_out)

            if not json_out:
                self.write("<p><small>xpu_id: " +
                           str(xpu_host.get_xpu_id()) + "</small><br>")
                self.write("<small>child_id: " +
                           str(xpu_host.get_child_id()) + "</small>")

        except Exception as e:
            self.write("<h2>Caught an exception:</h2><code>" +
                       str(e) + "</code>")

        if not json_out:
            self.write("</body></html>")

    def print_proto(self, histo_proto, json=False):
        first_hist = True
        if json:
            self.write('{"histogram": [')

        for hist in histo_proto.histograms:
            if json:
                if not first_hist:
                    self.write(',')
                first_hist = False
                self.write(
                    '{"name":"libruntime", "duration_sec": 60, "items":[')
            else:
                self.write(
                    '<h3>libruntime stats for the last {} seconds</h3>'.format(
                        hist.duration_sec))
                self.write('<table>')
                self.write('<tr><th style="padding - right: 2em; ">Name</th>')
                self.write('<th class="headerCell">Count</th>')
                self.write('<th class="headerCell">Mean suspensions</th>')
                self.write('<th class="headerCell">Mean suspended time</th>')
                self.write('<th class="headerCell">Mean infra locking time</th>')
                self.write('<th class="headerCell">Mean duration</th>')
                self.write('<th class="headerCell">StdDev for duration</th>')
                self.write('<th class="headerCell">')
                self.write('95th percentile duration</th>')
                self.write('<th class="headerCell">')
                self.write('99th percentile</th>')
                self.write('</tr>')

            first_item = True
            total_op_count = 0
            for item in hist.items:
                if json:
                    if not first_item:
                        self.write(',')
                    first_item = False
                    self.write('{')
                else:
                    self.write('<tr>')

                # Print out the name and sample count.
                total_op_count += item.count
                if json:
                    self.write("\"Name\":\"" + item.name + "\",")
                    self.write("\"Count\":" + str(item.count) + ",")
                else:
                    self.write("<td>" + item.name + "</td>")
                    self.write('<td style="text-align:right">' +
                               str(item.count) + "</td>")

                # Print out the suspension stats.
                if json:
                    self.write('"Suspensions":{0:.2f},'.format(
                        item.mean_suspensions))
                    self.write('"Mean suspended time":' +
                               str(item.mean_suspended_time_us) + ",")
                    self.write('"Mean infra locking time":' +
                               str(item.mean_locking_time_us) + ",")
                else:
                    self.write(
                        '<td style="text-align:right">{0:.2f}</td>'.format(
                            item.mean_suspensions))
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.mean_suspended_time_us) +
                               "</td>")
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.mean_locking_time_us) + "</td>")

                # Print out the duration stats.
                if json:
                    self.write('"Mean duration":' +
                               str(item.mean_duration_us) + ",")
                    self.write('"StdDev duration":' +
                               str(item.duration_stddev) + ",")
                    self.write('"95th percentile":' +
                               str(item.duration_95th_us) + ",")
                    self.write('"99th percentile":' +
                               str(item.duration_99th_us))
                else:
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.mean_duration_us) +
                               "</td>")
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.duration_stddev) +
                               "</td>")
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.duration_95th_us) +
                               "</td>")
                    self.write('<td style="text-align:right">' +
                               printable_duration(item.duration_99th_us) +
                               "</td>")

                if json:
                    self.write("}")
                else:
                    self.write("</tr>")

            if json:
                self.write("]}")
            else:
                self.write("</table>")

        if json:
            self.write(']}')


def main(in_blob):
    json_obj = json.loads(in_blob)

    base_port = 0
    api = XcalarApi(bypass_proxy=True)
    max_retry = 100
    cur_try = 0
    while True:
        try:
            config = api.getConfigParams()
            for ii in range(0, config.numParams):
                if config.parameter[ii].paramName == "RuntimePerfPort":
                    base_port = int(config.parameter[ii].paramValue)
            break
        except Exception as e:
            if cur_try >= max_retry:
                raise e
            logger.error("Unable to lookup config params due to error: {}. Trying again after 5 second sleep.".format(str(e)))
            time.sleep(5)
            cur_try += 1

    assert(base_port > 0)
    port = base_port + xpu_host.get_xpu_id()
    logger.info("Listening on port {}...".format(port))
    httpd = HTTPServer(('', port), Handler)
    httpd.serve_forever()


def printable_duration(duration_us):
    rv = ''

    minutes = int(duration_us / 1000000 / 60)
    if minutes > 0:
        rv += str(minutes) + " min "

    seconds = int(duration_us / 1000000) - minutes * 60
    if seconds > 0:
        rv += str(seconds) + " s "

    if minutes > 0:
        return rv

    ms = int(duration_us / 1000) - seconds * 1000
    if ms > 0:
        rv += str(ms) + " ms"
    else:
        rv = str(duration_us) + " &mu;s"

    return rv

# Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import sys
import os
import logging
import random
import string
import tarfile
import json
import io
import csv
from contextlib import contextmanager

import click

import xcalar.xc2.fixworkbook
import xcalar.compute.util.cluster
import xcalar.compute.util.upgrade
from xcalar.compute.util.crc32c import crc32
from xcalar.compute.util.utils import XcUtil

from xcalar.external.client import Client
from xcalar.external.LegacyApi.WorkItem import WorkItemQueryState, WorkItemQueryDelete
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT
from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.util.utils import build_sorted_node_map

HEALTH_TEMPLATE = "{name:18}{status:10}{pid:<8}{uptime:15}"
# Making this global variable as client will be used
# across all the commands.
client = None

# this is to report error without the traceback
sys.tracebacklimit = 0


@click.group()
@click.option('-v', '--verbose', count=True)
@click.version_option()    # let it get version info from package metadata
def cli(verbose):
    """Xcalar Cluster Controller

    Manage your Xcalar Compute Engine cluster."""
    level_warning = False
    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    elif verbose > 2:
        level = logging.DEBUG
        level_warning = True

    logging.basicConfig(level=level, format="[xc2][%(name)s] %(message)s")
    if level_warning:
        logging.warning(
            "verbose level {} unspported; falling back to level {}".format(
                verbose, 2))


def _pretty_print_status(status, err=False):
    header = {
        "name": "NAME",
        "status": "STATUS",
        "pid": "PID",
        "uptime": "UPTIME"
    }
    colors = {
        xcalar.compute.util.cluster.ProcessState.DOWN: "red",
        xcalar.compute.util.cluster.ProcessState.STARTED: "yellow",
        xcalar.compute.util.cluster.ProcessState.UP: "green",
    }

    click.echo(HEALTH_TEMPLATE.format(**header), err=err)

    for p_name, proc in status.items():
        color = colors[proc["status"]]
        # change status to its string version
        proc["status"] = proc["status"].value

        click.echo(
            click.style(HEALTH_TEMPLATE.format(**proc), fg=color), err=err)


def _establish_client():
    global client
    if client is not None:
        return

    xce_target_url = os.getenv("XCE_TARGET_URL", None)
    xce_credentials_file = os.getenv("XCE_CREDENTIAL_FILE", None)
    credentials_json_str = os.getenv("XCE_CREDENTIALS", None)

    xce_credentials = None
    if credentials_json_str is not None:
        try:
            xce_credentials = json.loads(credentials_json_str)
        except json.decoder.JSONDecodeError as ex:
            raise ValueError(
                "XCE_CREDETIALS environment variable should be valid json"
            ) from ex

    client = Client(
        url=xce_target_url,
        client_secrets=xce_credentials,
        client_secrets_file=xce_credentials_file,
        save_cookies=True)


@cli.group(short_help="manage cluster lifecycle")
def cluster():
    """Manage cluster lifecycle

    Based off of a config file, bring a cluster up or down and gain information
    about any and all running Xcalar services."""
    pass


@cluster.command(short_help="show cluster health")
def health():
    """Show cluster health

    Shows the status of any running Xcalar processes. Where available, also
    shows whether a service is responding to requests"""
    cluster = xcalar.compute.util.cluster.detect_cluster()

    status = cluster.status()
    _pretty_print_status(status)

    if not cluster.is_up():
        # https://github.com/pallets/click/issues/270
        # Click doesn't seem to have a builtin for exit codes
        sys.exit(1)
    # https://github.com/pallets/click/issues/270
    # Click doesn't seem to have a builtin for exit codes
    sys.exit(0)


@cluster.command(short_help="start cluster")
@click.option('--num-nodes', default=3)
@click.option('--config-file', type=click.Path(exists=True))
@click.option(
    '--guardrails-args',
    default=None,
    help="Guardrails args, Eg. \"-dt30 -T30\"")
@click.option('--timeout', default=100)
def start(num_nodes, config_file, guardrails_args, timeout):
    """Start a Xcalar cluster

    Starts a Xcalar cluster and waits until the cluster is functional. If a
    Xcalar cluster is already running, do nothing and return. """
    old_cluster = xcalar.compute.util.cluster.detect_cluster()
    if old_cluster.is_up():
        logging.warning("cluster is already running")
        return

    # config_file can be None, which will cause it to auto-detect a config file
    cluster = xcalar.compute.util.cluster.DevCluster(
        guardrailsArgs=guardrails_args,
        num_nodes=num_nodes,
        config_path=config_file)
    try:
        cluster.start(timeout=timeout)
    except RuntimeError as e:
        if "timed out waiting for cluster to start" in str(e):
            _pretty_print_status(cluster.status(), err=True)
        raise

    status = cluster.status()
    _pretty_print_status(status)


@cluster.command(short_help="stop running cluster")
def stop():
    """Stops a running Xcalar cluster

    Stops a Xcalar cluster and waits until the cluster is completely down. If
    no Xcalar cluster is currently running, do nothing and return"""
    cluster = xcalar.compute.util.cluster.detect_cluster()
    if cluster.is_fully_down():
        logging.warning("no running cluster found")
        return

    cluster.stop()

    status = cluster.status()
    _pretty_print_status(status)


@cli.group(short_help="fix workbooks")
def fix_workbooks():
    """Fix damaged workbooks and other tar.gz files with a similar file
    structure

    Functionalities:

    1. Extract UDF module from a given workbook (workbook name or local path to
    the workbook).

    2. Fix online workbooks or local tar.gz files.

    When fixing workbooks online, it downloads the workbooks, fixes them, and
    uploads the new fixed workbooks. This is for workbooks only.

    When fixing locally, it fixes the local tar.gz files (workbooks, dataflows,
    published tables) in the path, and creates new fixed tar.gz files.
    """
    pass


@fix_workbooks.command(short_help="extract UDF from a local workbook")
@click.argument("local-path", nargs=1)
@click.argument("module-name", nargs=1)
def extract_local(local_path, module_name):
    """Extract UDF module from a local workbook

    Path to the local workbook can be a file or a folder.
    """
    fix_tool = xcalar.xc2.fixworkbook.FixWorkbook(
        local_path=[local_path], module_name=module_name)
    fix_tool.fix_workbook()


@fix_workbooks.command(short_help="extract UDF from a workbook online")
@click.argument("workbook-name", nargs=1)
@click.argument("module-name", nargs=1)
@click.option("--username", prompt=True)
@click.option('--password', prompt=True, hide_input=True)
def extract_online(workbook_name, module_name, username, password):
    """Extract UDF modlue from a workbook online

    """
    fix_tool = xcalar.xc2.fixworkbook.FixWorkbook(
        username=username,
        password=password,
        workbook_name=workbook_name,
        module_name=module_name)
    fix_tool.fix_workbook()


@fix_workbooks.command(short_help="fix local workbooks")
@click.argument("local-path", nargs=-1, type=click.Path(exists=True))
@click.option("--udf-path", help="path to the module to be added or replaced")
@click.option("--suffix", help="suffix for the fixed workbooks")
@click.option(
    "--recursive/--no-recursive",
    default=False,
    help="fix execute-retina nodes inside workbooks recursively")
@click.option("--replace", help="string to be replaced by \"-\"")
def fix_local(local_path, udf_path, suffix, recursive, replace):
    """Fix local workbooks

    Fix a misssing UDF.
    Fix column names with double dashes.
    Will fix embedded batch dataflow if required.

    Path to the local workbooks (to be fixed) can be a file or a folder. Wild
    card matching is supported.
    """
    fix_tool = xcalar.xc2.fixworkbook.FixWorkbook(
        local_path=local_path,
        recursive=recursive,
        udf_path=udf_path,
        suffix=suffix,
        replace=replace)
    fix_tool.fix_workbook()


@fix_workbooks.command(short_help="fix workbooks online")
@click.argument("workbook-name", nargs=1)
@click.option("--username", prompt=True)
@click.option('--password', prompt=True, hide_input=True)
@click.option("--udf-path", help="path to the module to be added or replaced")
@click.option("--suffix", help="suffix for the fixed workbooks")
@click.option(
    "--recursive/--no-recursive",
    default=False,
    help="fix execute-retina nodes inside workbooks recursively")
@click.option("--replace", help="string to be replaced by \"-\"")
def fix_online(workbook_name, username, password, udf_path, suffix, recursive,
               replace):
    """Fix workbooks online

    Fix a misssing UDF.
    Fix column names with double dashes.
    Will fix embedded batch dataflow if required.

    Workbook (to be fixed) names can be separated by comma. Wild card matching
    is supported.
    """
    fix_tool = xcalar.xc2.fixworkbook.FixWorkbook(
        username=username,
        password=password,
        workbook_name=workbook_name,
        recursive=recursive,
        udf_path=udf_path,
        suffix=suffix,
        replace=replace)
    fix_tool.fix_workbook()


@cli.command(short_help="upgrade")
@click.option("--config", help="absolute path to config file")
@click.option("--verbose/--no-verbose", default=False, help="verbose output")
@click.option("--force/--no-force", default=False, help="force upgrade")
def upgrade(config, verbose, force):
    """Upgrade on disk workbooks and batch dataflows to dataflow 2.0 format

    Make sure the cluster is down before running this.
    """

    upgrade_tool = xcalar.compute.util.upgrade.UpgradeTool(
        config, verbose, force)
    upgrade_tool.start()


@cli.group(short_help="workbook commands")
def workbook():
    """Workbook commands to list and execute.

    Functionalities:

    1. List the dataflows for a given workbook name or file.

    2. Execute dataflow given it's name and workbook name or file.

    """
    _establish_client()


@workbook.command(short_help="List dataflows")
@click.option("--workbook-name", help="workbook name")
@click.option("--workbook-file", help="workbook file")
def show(workbook_name, workbook_file):
    """
    Shows a list of dataflows from a workbook or workbook file.
    If both or neither are provided the command fails.
    """
    if (workbook_name is None
            and workbook_file is None) or (workbook_name is not None
                                           and workbook_file is not None):
        raise ValueError(
            "Either workbook name or workbook file must be provided and not both!"
        )
    if workbook_file:
        # Create a workbook with a random name in order to upload the
        # workbook file content, list the dataflows, and then delete
        # workbook.
        with open(workbook_file, 'rb') as wb_fp:
            wb_contents = wb_fp.read()
        random_str = "".join(
            random.choice(string.ascii_letters) for i in range(10))
        workbook_name = os.path.basename(workbook_file).split(
            ".")[0] + "_list_dataflows_" + random_str
        workbook = client.upload_workbook(workbook_name, wb_contents)
    else:
        workbook = client.get_workbook(workbook_name=workbook_name)
    try:
        dataflow_names = workbook.list_dataflows()
    finally:
        if workbook_file:
            workbook.delete()
    if len(dataflow_names) == 0:
        click.echo("No dataflows found!")
    for num, df_name in enumerate(dataflow_names):
        click.echo(f"{num+1}. {df_name}")


def _parse_params(params):
    """
    Parses params string of "key1=value1,key2=value2" pattern
    into a dictionary. Also takes into consideration for escape
    delimiters in params
    """

    def split_with_escape(string, delim):
        token_list = []
        text = ""
        for token in string.split(delim):
            if token and token[-1] == '\\':
                text += token[:-1] + delim
            else:
                text += token
                token_list.append(text)
                text = ""
        return token_list

    params_dict = {}
    if not params:
        return params_dict
    for param in split_with_escape(params, ','):
        try:
            token_list = split_with_escape(param, '=')
            token_list_len = len(token_list)
            if token_list_len == 1:
                params_dict[token_list[0]] = None
            elif token_list_len == 2:
                params_dict[token_list[0]] = token_list[1]
            else:
                raise ValueError(f"Invalid param {param} found!")
        except ValueError as err:
            raise ValueError(
                "Failed to parse params, params should be comma separated <key>=<value> pairs"
            ) from err
    return params_dict


def _publish_table(op_api, sess_tab_name, pub_tab_name, drop_if_exists):
    if drop_if_exists:
        # delete the published table if exists first
        with XcUtil.ignored_xc_status_codes(
                StatusT.StatusPubTableNameNotFound):
            op_api.unpublish(pub_tab_name)
    op_api.publish(sess_tab_name, pub_tab_name, dropSrc=True)


@workbook.command(
    short_help="Runs the specified dataflow from a workbook or workbook file.")
@click.option("--workbook-name", help="workbook name")
@click.option("--workbook-file", help="workbook file path")
@click.option("--dataflow-name", help="dataflow name")
@click.option(
    "--resultant-table-name",
    help="resultant table name of dataflow execution")
@click.option("--non-optimized", is_flag=True, help="Run as non optimized")
@click.option("--sync", is_flag=True, help="Run synchronously")
@click.option("--query-name", help="query name to run this dataflow as")
@click.option(
    "--params",
    help=("params for dataflow should be in comma separated <key>=<value> "
          "format. Ex: key1=val1,key2=val2"))
@click.option(
    "--pin-results", is_flag=True, help="pin results of the dataflow run.")
@click.option(
    "--sched-name",
    help="sched name to run on; valid options: Sched0, Sched1, Sched2")
@click.option(
    "--clean-job-state",
    is_flag=True,
    help="clean job state after successful run")
@click.option(
    "--fail-if-table-exists",
    is_flag=True,
    help="Fail the run if table already exists, else overwrite the old table.")
def run(workbook_name, workbook_file, dataflow_name, resultant_table_name,
        non_optimized, sync, query_name, params, pin_results, sched_name,
        clean_job_state, fail_if_table_exists):
    """
    Runs the specified dataflow from a workbook or workbook file.
    If both are provided, workbook file will be uploaded by given workbook name.
    """
    if workbook_name is None and workbook_file is None:
        raise ValueError(
            "Either workbook name or workbook file must be provided!")
    if sched_name and sched_name not in client.runtime().Scheduler.__members__:
        raise ValueError("Invalid sched name; Valid values are {}".format(
            list(client.runtime().Scheduler.__members__.keys())))
    if workbook_file:
        with open(workbook_file, 'rb') as wb_fp:
            wb_contents = wb_fp.read()
        if not workbook_name:
            workbook_name = os.path.basename(workbook_file).split(".")[0]
        workbook = client.upload_workbook(workbook_name, wb_contents)
    else:
        workbook = client.get_workbook(workbook_name=workbook_name)

    if not dataflow_name:
        dataflow_names = workbook.list_dataflows()
        if len(dataflow_names) == 1:
            dataflow_name = dataflow_names[0]
        elif len(dataflow_names) == 0:
            raise ValueError("There are no dataflows in workbook to run!")
        else:
            dataflows_str = "\n".join(dataflow_names)
            raise ValueError(
                "More than one dataflow available, choose one from following to run,\n{}"
                .format(dataflows_str))

    params_dict = _parse_params(params)
    dataflow = workbook.get_dataflow(
        dataflow_name=dataflow_name, params=params_dict)

    pub_result_map = dataflow.get_publish_result_map()
    result_sess_tab_name = None

    sess = workbook.activate()
    xc_api = XcalarApi(auth_instance=client.auth)
    xc_api.setSession(sess)
    op_api = Operators(xc_api)
    new_pub_table_names = []
    if len(pub_result_map) > 0:
        if non_optimized:
            raise ValueError(
                "Not supported to run dataflow containing 'Publish Table' node as non-optimized!"
            )
        # XXX currently we only support export to one table in optimized mode
        if len(pub_result_map) > 1:
            raise RuntimeError(
                "Not supported to create more than one table in optimized mode!"
            )
        sess_pub_tab_map = next(iter(pub_result_map.items()))
        if resultant_table_name is None:
            resultant_table_name = sess_pub_tab_map[1]
        resultant_table_name = resultant_table_name.upper()
        result_sess_tab_name = sess_pub_tab_map[0]
        new_pub_table_names = [resultant_table_name]
    else:
        # whatever user passes will be used as
        # resultant table name created in session
        result_sess_tab_name = resultant_table_name
        new_pub_table_names = pub_result_map.values()

    # check if publish table already exists and fail
    # before running dataflow, if user passes fail-if-table-exists
    if fail_if_table_exists is True and len(pub_result_map) > 0:
        curr_tables = {
            tab.name
            for tab in op_api.listPublishedTables('*').tables
        }
        for pub_tab_name in new_pub_table_names:
            if pub_tab_name in curr_tables:
                raise RuntimeError(f"Table {pub_tab_name} already exists")

    sched_name_ = None
    if sched_name:
        sched_name_ = client.runtime().Scheduler[sched_name]
    result = sess.execute_dataflow(
        dataflow,
        optimized=not non_optimized,
        table_name=result_sess_tab_name,
        query_name=query_name,
        params=params_dict,
        is_async=not sync,
        pin_results=pin_results,
        sched_name=sched_name_,
        clean_job_state=clean_job_state)

    # publish the table to global namespace to access table from BI tools
    if len(pub_result_map) > 0 and not non_optimized:
        _publish_table(op_api, result_sess_tab_name, resultant_table_name,
                       not fail_if_table_exists)
    else:
        for xc_tab_name, pub_tab_name in pub_result_map.items():
            _publish_table(op_api, xc_tab_name, pub_tab_name,
                           not fail_if_table_exists)

    click.echo(result)


@workbook.command(short_help="Deletes the workbook given workbook name.")
@click.argument("workbook-name", type=str)
def delete(workbook_name):
    try:
        workbook = client.get_workbook(workbook_name=workbook_name)
        workbook.delete()
        click.echo("Workbook deleted!")
    except ValueError as ex:
        click.echo("Workbook doesn't exists")


@cli.group(short_help="dataflow commands")
def dataflow():
    """Dataflow util commands.

    Functionalities:

    1. extract_kvstore - Extracts kvstoreInfo.json as a JSON object into stdout or as a specified input file.

    2. update_kvstore - Given a dataflow file and a json kvstore file as input, generates a new dataflow tar.gz file to a specified path.

    """
    _establish_client()


def _parse_json(json_string):
    content = None
    try:
        content = json.loads(json_string)
    except (json.decoder.JSONDecodeError, TypeError) as ex:
        return json_string
    if not isinstance(content, dict):
        return content
    for key, value in content.items():
        if isinstance(value, list):
            json_values = []
            for sub_value in value:
                json_values.append(_parse_json(json.dumps(sub_value)))
            content[key] = json_values
        else:
            content[key] = _parse_json(value)
    return content


# Json stringify is done intentionally only one level
# As XD accepts one level stringification
def _stringify_json(json_dict):
    if not isinstance(json_dict, dict):
        return json.dumps(json_dict, separators=(',', ':'))
    for key, value in json_dict.items():
        json_dict[key] = json.dumps(value, separators=(',', ':'))
    return json.dumps(json_dict, separators=(',', ':'))


@dataflow.command(short_help="dataflow util commands")
@click.argument("dataflow-file", type=click.Path(exists=True))
@click.option(
    "--extract-to",
    type=click.Path(file_okay=True),
    help="extract the dataflow file")
def extract_kvstore(dataflow_file, extract_to):
    if not tarfile.is_tarfile(dataflow_file):
        raise click.BadParameter(
            'dataflow-file should be of format xlrdf.tar.gz only')

    file_to_extract = 'workbook/kvstoreInfo.json'
    dataflow_json_file = None
    with tarfile.open(name=dataflow_file, mode="r:gz") as tar:
        dataflow_json_file = tar.extractfile(file_to_extract)
        json_dataflow = json.load(dataflow_json_file)
    json_dataflow = _parse_json(json.dumps(json_dataflow))
    if extract_to:
        with open(extract_to, 'w') as fp:
            json.dump(json_dataflow, fp, indent=4)
    else:
        json.dump(json_dataflow, sys.stdout, indent=4)


@dataflow.command(short_help="dataflow util commands")
@click.argument("dataflow-file", type=click.Path(exists=True))
@click.argument("json-file", type=click.Path(exists=True))
@click.option(
    "--new-dataflow-file",
    type=click.Path(file_okay=True),
    help="new dataflow file path")
def update_kvstore(dataflow_file, json_file, new_dataflow_file):
    if not tarfile.is_tarfile(dataflow_file):
        raise click.BadParameter(
            'dataflow-file should be of format xlrdf.tar.gz only')
    try:
        with open(json_file, 'r') as fp:
            content = json.load(fp)
    except (json.decoder.JSONDecodeError, TypeError) as ex:
        raise click.BadParameter('json-file should be a valid json file')

    # create a new file name at the same path as old
    if not new_dataflow_file:
        base_name = os.path.basename(dataflow_file).split('.')
        new_dataflow_file = base_name[0] + '_generated' + (
            '.' + '.'.join(base_name[1:])) if len(base_name) > 1 else ''

    file_to_update = 'workbook/kvstoreInfo.json'
    checksum_file = 'workbook/workbookChecksum.json'
    with tarfile.open(
            name=dataflow_file, mode="r:gz") as input_tar, tarfile.open(
                name=new_dataflow_file, mode="w:gz") as output_tar:
        checksum = {}
        # add kvstore file
        kvstore_tar = tarfile.TarInfo(file_to_update)
        kvstore_contents = io.BytesIO(_stringify_json(content).encode())
        kvstore_contents.seek(0, io.SEEK_END)
        kvstore_tar.size = kvstore_contents.tell()
        kvstore_contents.seek(0, io.SEEK_SET)
        checksum[kvstore_tar.name] = "{:0>8}".format(
            hex(crc32(kvstore_contents))[2:])
        kvstore_contents.seek(0)
        output_tar.addfile(kvstore_tar, kvstore_contents)
        # add all other members except checksum
        for member in input_tar:
            if member.name != file_to_update and member.name != checksum_file:
                member_contents = input_tar.extractfile(member.name)
                if member_contents:
                    checksum[member.name] = "{:0>8}".format(
                        hex(crc32(member_contents))[2:])
                    member_contents.seek(0)
                output_tar.addfile(member, member_contents)

        # add checksum file
        checksum_tar = tarfile.TarInfo(checksum_file)
        checksum_conents = io.BytesIO(json.dumps(checksum, indent=4).encode())
        checksum_conents.seek(0, io.SEEK_END)
        checksum_tar.size = checksum_conents.tell()
        checksum_conents.seek(0, io.SEEK_SET)
        output_tar.addfile(checksum_tar, checksum_conents)
    click.echo(
        'dataflow generated is written to {} file'.format(new_dataflow_file))


@contextmanager
def _open_stats_file(stats_file):
    fh = None
    if stats_file:
        fh = open(stats_file, 'w')
    else:
        fh = sys.stdout
    yield fh
    if stats_file:
        fh.close()


@dataflow.command(short_help="fetch dataflow run time stats")
@click.argument("dataflow-name")
@click.option(
    "--stats-file",
    type=click.Path(file_okay=True),
    help="collected stats to write to")
@click.option(
    "--delete-query", is_flag=True, help="Delete dataflow run instance.")
def stats(dataflow_name, stats_file, delete_query):
    workItem = WorkItemQueryState(dataflow_name)
    rv = client._legacy_xcalar_api.execute(workItem)
    sortedGraph = build_sorted_node_map(rv.queryGraph.node)
    with _open_stats_file(stats_file) as fh:
        # for display purpose, using tab else comma
        if stats_file:
            delimiter = ','
        else:
            delimiter = '\t'
        csvWriter = csv.writer(
            fh, delimiter=delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)

        header_written = False
        # cache node rows counts with output table names
        # to use for export operation to a file
        cache_node_row_counts = {}
        for idx, nodeId in enumerate(sortedGraph):
            node = sortedGraph[nodeId]
            record = {}
            record["seq_num"] = idx + 1
            record["query_name"] = dataflow_name
            record["query_status"] = StatusT._VALUES_TO_NAMES[rv.queryStatus]
            record["query_state"] = QueryStateT._VALUES_TO_NAMES[rv.queryState]
            record["total_time_elapsed_millisecs"] = rv.elapsed.milliseconds
            record["completed_operations"] = rv.numCompletedWorkItem
            record["failed_operations"] = rv.numFailedWorkItem
            inputTables = []
            outputTables = []
            for dagID in node.parents:
                inputTables.append(sortedGraph[dagID].name.name)
            for dagID in node.children:
                outputTables.append(sortedGraph[dagID].name.name)
            record["node_name"] = node.name.name
            record["input_tables"] = ":".join(inputTables)
            record["output_tables"] = ":".join(outputTables)
            record["operation_name"] = XcalarApisT._VALUES_TO_NAMES[node.api]
            record["operation_state"] = DgDagStateT._VALUES_TO_NAMES[node.
                                                                     state]
            record["node_time_elapsed_millisecs"] = node.elapsed.milliseconds
            record["node_start_time"] = node.startTime
            record["node_end_time"] = node.endTime
            # When you export to a file there is no table, so the row count
            # will always be zero. We correct this by getting the row
            # count from the table which we exported from.
            #
            # If there were no output tables,
            # and we are running an export:
            if len(outputTables
                   ) == 0 and record["operation_name"] == 'XcalarApiExport':
                # Values of -1 mean we were not able to find the table
                # that we are exporting from in the records.
                record["total_row_count"], record[
                    "rows_per_node"] = cache_node_row_counts.get(
                        record['node_name'], (-1, -1))
            else:
                record["total_row_count"] = node.numRowsTotal
                record["rows_per_node"] = ":".join(
                    str(e) for e in node.numRowsPerNode)
            cache_node_row_counts[record["output_tables"]] = (
                record["total_row_count"], record["rows_per_node"])
            record["total_node_count"] = node.numNodes
            record['node_comment'] = ""
            if node.comment != '':
                commentDict = json.loads(node.comment)
                if 'userComment' in commentDict:
                    record['node_comment'] = '"' + commentDict[
                        'userComment'] + '"'
            if not header_written:
                csvWriter.writerow(record.keys())
                header_written = True
            csvWriter.writerow(record.values())

    # Delete the query if asked to
    if delete_query:
        workItem = WorkItemQueryDelete(dataflow_name)
        client._legacy_xcalar_api.execute(workItem)


@cli.command(short_help="authenticate a user to use SDK")
@click.option("--username", prompt=True)
@click.option("--password", prompt=True, hide_input=True)
def login(username, password):
    global client
    xce_target_url = os.getenv("XCE_TARGET_URL", None)
    client_secrets = {"xiusername": username, "xipassword": password}
    client = Client(
        url=xce_target_url, client_secrets=client_secrets, save_cookies=True)
    client.auth.login()

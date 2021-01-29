#!/usr/bin/env python

import os
import json
import argparse
import time
import sys
import logging

from data_gen_dataflow import data_gen_dataflow
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-5s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

udf_module_name = "customer4_data_gen_udf"
udf_func_name = "gen_customer4_data"
mem_target = "generated"
kafka_export_driver = "data_gen_kafka_export"
base_export_driver = "snapshot_parquet"
# These are the imd tables used in customer4's ValuedAsset refiner dataflow
imd_tables = [
    'GSEntlInflightTB', 'GSEntlPosnTB', 'PWM_Prices', 'PositionsSTB',
    'Taxlots_Cost', 'PWM_Unit_FI_Accruals', 'BDADailyBalAccrlTB'
]
min_row_count = 10000

batch_counter_key = "XCE_TEST_RIG_DATA_GEN_BATCH_COUNTER"

here = os.path.abspath(os.path.dirname(__file__))
os.environ["XLR_PYSDK_VERIFY_SSL_CERT"] = "false"

client = None
session = None
xcalar_root = None
global_kvstore = None


def cluster_setup(args):
    global client, session, xcalar_root, global_kvstore

    # setup cluster client and session
    url = "https://{}:{}".format(args.xcalarHost, args.apiport)
    username = args.xcalarUser
    password = args.xcalarPass
    client = Client(
        url=url,
        client_secrets={
            "xiusername": username,
            "xipassword": password
        })
    session = None
    try:
        session = client.get_session(args.sessionName)
    except Exception:
        session = client.create_session(args.sessionName)

    for param in client.get_config_params():
        if param["param_name"] == "XcalarRootCompletePath":
            xcalar_root = param["param_value"]
    assert xcalar_root is not None

    global_kvstore = client.global_kvstore()

    # add udfs
    udfs = {
        kafka_export_driver: 'udfs/data_gen_kafka_export.py',
        udf_module_name: 'udfs/customer4_data_gen_udf.py'
    }
    for udf_mod_name, udf_source_path in udfs.items():
        with open(os.path.join(here, udf_source_path)) as fp:
            try:
                client.get_udf_module(udf_mod_name).delete()
            except Exception as ex:
                logger.info('warn: {}'.format(ex))
            logger.info('creating udf module {}'.format(udf_mod_name))
            client.create_udf_module(udf_mod_name, fp.read())

    # add targets
    client.add_data_target(target_name=mem_target, target_type_id="memory")


def gen_table_data(tab_name,
                   schema,
                   num_records,
                   dir_path,
                   batch_id,
                   seed,
                   customer4_dir,
                   session_name,
                   target_name,
                   user_name,
                   export_driver_name="snapshot_parquet",
                   kafka_server=None):
    ingest_func_name = "{}:{}".format(udf_module_name, udf_func_name)
    ingest_params = {
        "schema_inputs": {
            "table_name":
                tab_name,
            "path_to_schemas":
                os.path.join(customer4_dir, "customer4_schemas.json"),
            "path_to_data_stats":
                os.path.join(customer4_dir, "{}.json".format(tab_name)),
            "seed":
                seed,
            "batch_id":
                batch_id,
            "total_row_count":
                num_records,
            "join_keys_only":
                False,
            "user_name":
                user_name,
            "session_name":
                session_name,
    # "path_to_contraints":
    #     os.path.join(customer4_dir, "refiner_tabs_contraints.json"),
        }
    }

    if export_driver_name == base_export_driver:
        export_params = {
            "target": target_name,
            "directory_path": dir_path,
            "file_base": "file_" + str(batch_id),
        }
    elif export_driver_name == kafka_export_driver:
        assert kafka_server, "kafka_server cannot be None"
        export_params = {
            'target':
                target_name,
            'kafka_server':
                kafka_server,
            'avsc_fname':
                '/netstore/controller/customer4.avsc',
            'kafka_params':
                "{\"dataset_name\" : \"" + tab_name +
                "\",\"batch_size\": 1000}",
            'kafka_topic':
                'JD_' + tab_name
        }
    else:
        assert False, "Only snapshot_parquet, data_gen_kafka_export export drivers are supported"
    # add entry_dt and batch_id to schema
    schema["BATCH_ID"] = "integer"
    schema["ENTRY_DT"] = "timestamp"
    qs, retina_str = data_gen_dataflow(
        schema,
        num_records,
        mem_target,
        ingest_func_name,
        ingest_params,
        export_driver_name,
        export_params,
    )
    df = Dataflow.create_dataflow_from_query_string(
        client, qs, optimized_query_string=retina_str)
    df._dataflow_name = tab_name
    session.execute_dataflow(df, optimized=True, is_async=False)


def gen_table_pkeys(tab_name, schema, num_records, batch_id, seed, customer4_dir,
                    target_name):
    pkeys_tab = "{}_pkeys".format(tab_name)
    try:
        tab = session.get_table(pkeys_tab)
        logger.info(
            f">> Reusing keys data with {tab.record_count()} records for {tab_name}.."
        )
        return
    except Exception:
        pass
    logger.info(
        f">> Generating keys data with {num_records} records for {tab_name}..")
    ingest_func_name = "{}:{}".format(udf_module_name, udf_func_name)
    ingest_params = {
        "schema_inputs": {
            "table_name":
                tab_name,
            "path_to_schemas":
                os.path.join(customer4_dir, "customer4_schemas.json"),
            "path_to_data_stats":
                os.path.join(customer4_dir, "{}.json".format(tab_name)),
            "seed":
                seed,
            "batch_id":
                batch_id,
            "total_row_count":
                num_records,
            "join_keys_only":
                True,
    # "path_to_contraints":
    #     os.path.join(customer4_dir, "refiner_tabs_contraints.json"),
        }
    }

    export_driver_name = "do_nothing"
    export_params = {}
    qs, retina_str = data_gen_dataflow(
        schema,
        num_records,
        mem_target,
        ingest_func_name,
        ingest_params,
        export_driver_name,
        export_params,
    )
    df = Dataflow.create_dataflow_from_query_string(
        client, qs, optimized_query_string=retina_str)
    df._dataflow_name = "{}_pkeys".format(tab_name)
    session.execute_dataflow(
        df,
        table_name="{}_pkeys".format(tab_name),
        optimized=True,
        is_async=False)


def generate_key_tables(schemas, batch_num, seed, customer4_dir, total_row_count,
                        target_name):
    for tab_name in schemas:
        tab_rec_count = max(
            min_row_count,
            (int(schemas[tab_name]["row_count"]) * num_records) //
            total_row_count,
        )
        schema = schemas[tab_name]["columns"]

        gen_table_pkeys(tab_name, schema, tab_rec_count, batch_num, seed,
                        customer4_dir, target_name)
    logger.info("Done!")


def clean_test_data(export_dir):
    logger.info(">>>>>>>>>>>>>> cleaning test data <<<<<<<<<<<<<")
    logger.info("dropping tables..")
    for tab in session.list_tables():
        if tab.is_pinned():
            tab.unpin()
        tab.drop(delete_completely=True)
    logger.info("cleaning kvstore test state..")
    try:
        global_kvstore.delete(batch_counter_key)
    except Exception as ex:
        logger.warn(f"Warning: {ex}")
    logger.info("done!")
    logger.info("please delete any previous data from '{}'".format(export_dir))
    logger.info("<<<<<<<<<<<<<<<<<<< Done >>>>>>>>>>>>>>>>>>>>>>\n")


def parse_user_args():
    parser = argparse.ArgumentParser(
        description="customer4 data generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-H",
        dest="xcalarHost",
        required=False,
        default="localhost",
        type=str,
        help="host of the Xcalar cluster",
    )
    parser.add_argument(
        '-a',
        dest='apiport',
        required=False,
        default=int(os.getenv('XCE_HTTPS_PORT','8443')),
        type=int,
        help='Xcalar API port (8443 for some dockers)')
    parser.add_argument(
        "-U",
        "--xcalarUser",
        dest="xcalarUser",
        help="username of xcalar",
        required=False,
        default="admin",
    )
    parser.add_argument(
        "-P",
        "--xcalarPass",
        dest="xcalarPass",
        help="password of xcalar",
        required=False,
        default="admin",
    )
    parser.add_argument(
        "-e",
        "--exportPath",
        dest="exportPath",
        help="path to where data generated should be written to",
        required=False)
    parser.add_argument(
        "-b",
        "--baseSize",
        dest="baseSize",
        help="number of records for base tables",
        required=False,
        type=int,
        default=min_row_count * 10,
    )
    parser.add_argument(
        "-d",
        "--deltaSize",
        dest="deltaSize",
        help="number of records for delta tables",
        required=False,
        type=int,
        default=min_row_count,
    )
    parser.add_argument(
        "-g",
        "--customer4Dir",
        dest="customer4Dir",
        help="customer4 files directory path",
        required=False,
        default="/netstore/datasets/customer4/pwm_pilot",
    )
    parser.add_argument(
        "-c",
        "--cleanUp",
        dest="cleanUp",
        help="cleans the test state",
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "-m",
        "--numBatches",
        dest="numBatches",
        help="number of batch should be generated",
        required=False,
        default="-1",
    )
    parser.add_argument(
        "-s",
        "--sessionName",
        dest="sessionName",
        help="sessionName to be generated",
        required=False,
        default="data_gen",
    )
    parser.add_argument(
        "-t",
        "--targetName",
        dest="targetName",
        help="target to be used",
        required=False,
        default="Default Shared Root",
    )
    parser.add_argument(
        "--baseExportDriver",
        dest="baseExportDriver",
        help="base export driver to be used",
        required=False,
        default=base_export_driver,
    )
    parser.add_argument(
        "--deltaExportDriver",
        dest="deltaExportDriver",
        help="delta export driver to be usedd",
        required=False,
        default=kafka_export_driver,
    )
    parser.add_argument(
        "-k",
        "--kafkaServer",
        dest="kafkaServer",
        required=False,
        default="localhost:9092",
        type=str,
        help="host:port of kafka broker",
    )
    parser.add_argument(
        "--deltasOnly",
        dest="deltasOnly",
        required=False,
        action="store_true",
        help="generate only deltas",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_user_args()

    # setup
    cluster_setup(args)
    if args.exportPath is None:
        args.exportPath = os.path.join(xcalar_root, "export")
    logger.info("@@@ args: \n {}".format(args))

    export_dir = args.exportPath
    if args.cleanUp:
        clean_test_data(export_dir)
        sys.exit(0)

    customer4_dir = args.customer4Dir
    # get previous batch num ran
    try:
        batch_num = int(global_kvstore.lookup(batch_counter_key)) + 1
        num_records = args.deltaSize
    except Exception as ex:
        logger.info("warn: {}".format(ex))
        batch_num = 1
        num_records = args.baseSize

    seed = int(time.time())
    logger.info("Random seed: {}".format(seed))

    with open(os.path.join(customer4_dir, "customer4_schemas.json"),
              "r") as customer4_schemas_file:
        schemas = json.load(customer4_schemas_file)

    export_driver = args.baseExportDriver
    total_row_count = sum([schemas[tab]["row_count"] for tab in schemas])
    generate_key_tables(schemas, batch_num, seed, customer4_dir, total_row_count,
                        args.targetName)
    if batch_num > 1 or args.deltasOnly:
        total_row_count = sum([
            schemas[tab]["row_count"] for tab in schemas if tab in imd_tables
        ])
        export_driver = args.deltaExportDriver
        num_records = args.deltaSize
    i = 1
    numBatches = int(args.numBatches)
    while i <= numBatches or numBatches == -1:
        logger.info(
            ">>>>>>>>>>>>>Batch number {}<<<<<<<<<<<<<<<".format(batch_num))
        logger.info("Export driver: {}".format(export_driver))
        if export_driver == kafka_export_driver:
            logger.info("kafka server: {}".format(args.kafkaServer))
        logger.info("Target: {}".format(args.targetName))
        for tab_name in schemas:
            if batch_num > 1 and tab_name not in imd_tables:
                # generate deltas only for IMD tables
                continue
            tab_rec_count = max(
                min_row_count * 10 if batch_num == 1 else min_row_count,
                (int(schemas[tab_name]["row_count"]) * num_records) //
                total_row_count,
            )
            schema = schemas[tab_name]["columns"]
            logger.info(">> Generating data {} records for {}..".format(
                tab_rec_count, tab_name))
            gen_table_data(
                tab_name,
                schema,
                tab_rec_count,
                os.path.join(export_dir, str(tab_name), str(batch_num)),
                batch_num,
                seed,
                customer4_dir,
                args.sessionName,
                args.targetName,
                user_name=args.xcalarUser,
                export_driver_name=export_driver,
                kafka_server=args.kafkaServer)
        global_kvstore.add_or_replace(
            batch_counter_key, str(batch_num), persist=True)
        batch_num += 1
        num_records = args.deltaSize
        export_driver = args.deltaExportDriver
        total_row_count = sum([
            schemas[tab]["row_count"] for tab in schemas if tab in imd_tables
        ])
        logger.info("=" * 80 + "\n")
        time.sleep(1)
        i += 1

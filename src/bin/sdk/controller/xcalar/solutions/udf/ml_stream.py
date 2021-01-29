import json
from xcalar.external.client import Client
import xcalar.container.context as ctx

import xcalar.container.cluster
import xcalar.container.table
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT
import logging
import numpy as np

from dateutil.parser import parse
import keras
import numpy as np
from keras import backend as K
import tensorflow as tf

logger = logging.getLogger('xcalar')
client = Client(bypass_proxy=True)
kvstore = client.global_kvstore()

graph = tf.get_default_graph()
config = tf.ConfigProto(intra_op_parallelism_threads=1, inter_op_parallelism_threads=1, allow_soft_placement=False, device_count = {'CPU': 1})
session = tf.Session(config=config)
K.set_session(session)
model = None
def predict_np(np_array, xpu_id):
    global model
    global session
    global graph
    with session.as_default(): 
        with graph.as_default():
            if not model:
                model = keras.models.load_model("/tmp/dl_model1.h5", compile=True)
            try:
                res = model.predict(np.array(np_array))
                return res
            except Exception as e:
                logger.info(f'clone xpu error:{xpu_id}: {e}')
                yield {'error':f'Keras error: {e}', 'xpu':xpu_id}

def predict(in_array, xpu_id):
    global model
    global session
    global graph
    with session.as_default(): 
        with graph.as_default():
            if not model:
                model = keras.models.load_model("/tmp/dl_model1.h5", compile=True)
            try:
                # logger.info(f'clone xpu in:{xpu_id}: {in_array}')
                res = model.predict(in_array)
                #logger.info(f'clone xpu out:{xpu_id}: {res[0]}')
                return res
            except Exception as e:
                logger.info(f'clone xpu predict error:{xpu_id}: {e}')
                
def rme(x, y, table_id, session_name, user_name):
    logger.info(f'clone xpu: here')
    cluster = xcalar.container.cluster.get_running_cluster()
    logger.info(f'clone xpu:{cluster.my_xpu_id}, started')
    # take master xpu of node 0
    node_0_master = cluster.local_master_for_node(0)
    if cluster.my_xpu_id == node_0_master:
        # assuming, session is present on master node
        client = Client(bypass_proxy=True, user_name=user_name)
        sess = client.get_session(session_name)
        tab = sess.get_table(table_id)
        xdb_id = tab._get_meta().xdb_id
        tab_info = {
            'xdb_id':
            xdb_id,
            'columns': [{'columnName': c, 'headerAlias': c}
                       for c in tab._get_meta().columns]
        }
        logger.info(f'{tab._get_meta().columns}')
        
        cluster.broadcast_msg(tab_info)
    else:
        tab_info = cluster.recv_msg()
    logger.info(f'clone xpu:{cluster.my_xpu_id}, ready to proceed')
    try:
        table = xcalar.container.table.Table(tab_info['xdb_id'], tab_info['columns'])
        rows = table.partitioned_rows(format=DfFormatTypeT.DfFormatJson)
        logger.info(str(table._partitioned_row_start_count()))
        INCREMENT = 1500
        
        start_row, num_rows = table._partitioned_row_start_count()
        logger.info(f'clone xpu:{cluster.my_xpu_id}, start_row: {start_row} | num_rows: {num_rows}')
        offset = 0
        while offset < num_rows:
            next_start = start_row + offset
            #logger.info(f'clone xpu:{cluster.my_xpu_id}, next_start:{next_start} | offset:{offset}')
            # next_inc = INCREMENT
            if offset + INCREMENT  > num_rows:
                INCREMENT = num_rows - offset
            
            #logger.info(f'clone xpu:{cluster.my_xpu_id}, increment:{INCREMENT}')
            rows = table._local_cursor(next_start, INCREMENT, INCREMENT, format=DfFormatTypeT.DfFormatJson)
            in_array = None
            for r in rows: 
                row = [r['DELIVERY_DAYS_LOG'],r['PAYMENT_LOG']]
                if in_array is None:
                    in_array = [row]
                else:
                    in_array.append(row)
            res = predict([in_array], cluster.my_xpu_id)
            logger.info(f'clone xpu:{cluster.my_xpu_id}, type:{type(res)}')
            i = 0
            for r in res:  
                yield {
                      'predict':r[0], 
                      #'xpu':cluster.my_xpu_id, 
                      'DELIVERY_DAYS_LOG':in_array[i][0],
                      'PAYMENT_LOG':in_array[i][1]
                      }
                i += 1
                
            offset += INCREMENT 
        
    except Exception as e:
        logger.info(f'clone xpu YIELD error:{cluster.my_xpu_id}, {e}')
        yield {'error':str(e), 'xpu':cluster.my_xpu_id}
    logger.info(f'clone xpu:{cluster.my_xpu_id}, finished')
            
            
            
def rme_np(x, y, table_id, session_name, user_name):
    cluster = xcalar.container.cluster.get_running_cluster()
    logger.info(f'clone xpu:{cluster.my_xpu_id}, started')
    # take master xpu of node 0
    node_0_master = cluster.local_master_for_node(0)
    if cluster.my_xpu_id == node_0_master:
        # assuming, session is present on master node
        client = Client(bypass_proxy=True, user_name=user_name)
        sess = client.get_session(session_name)
        tab = sess.get_table(table_id)
        xdb_id = tab._get_meta().xdb_id
        tab_info = {
            'xdb_id':
            xdb_id,
            'columns': [{'columnName': c, 'headerAlias': c}
                       for c in tab._get_meta().columns]
        }
        logger.info(f'{tab._get_meta().columns}')
        
        cluster.broadcast_msg(tab_info)
    else:
        tab_info = cluster.recv_msg()
    logger.info(f'clone xpu:{cluster.my_xpu_id}, ready to proceed')
    try:
        table = xcalar.container.table.Table(tab_info['xdb_id'], tab_info['columns'])
        rows = table.partitioned_rows(format=DfFormatTypeT.DfFormatJson)
        logger.info(str(table._partitioned_row_start_count()))
        INCREMENT = 1000        
        #for r in rows:
        #    yield {**r, 'xpu':cluster.my_xpu_id}
        #return
    
        start_row, num_rows = table._partitioned_row_start_count()
        logger.info(f'clone xpu:{cluster.my_xpu_id}, start_row: {start_row} | num_rows: {num_rows}')
        offset = 0
        while offset < num_rows:
            next_start = start_row + offset
            #logger.info(f'clone xpu:{cluster.my_xpu_id}, next_start:{next_start} | offset:{offset}')
            if offset + INCREMENT  > num_rows:
                INCREMENT = num_rows - offset
            
            #logger.info(f'clone xpu:{cluster.my_xpu_id}, increment:{INCREMENT}')
            rows = table._local_cursor(next_start, INCREMENT, INCREMENT, format=DfFormatTypeT.DfFormatJson)
            np_array = None
            for r in rows: 
                np_row = np.array([r['DELIVERY_DAYS_LOG'],r['PAYMENT_LOG']])
                if np_array is None:
                    np_array = np_row
                else:
                    np_array = np.vstack([np_array, np_row])
            res = predict(np_array, cluster.my_xpu_id)
            i = 0
            for r in res:  
                yield {
                      'predict':r[0], 
                      #'xpu':cluster.my_xpu_id, 
                      'DELIVERY_DAYS_LOG':np_array[i][0],
                      'PAYMENT_LOG':np_array[i][1]
                      }
                i += 1
                
            offset += INCREMENT 

        
    except Exception as e:
        logger.info(f'clone xpu error:{cluster.my_xpu_id}, {e}')
        yield {'error':str(e), 'xpu':cluster.my_xpu_id}
    logger.info(f'clone xpu:{cluster.my_xpu_id}, finished')

# app -load kafka-base:1 -parser /sharedUDFs/rme:rme -args {"table_id":"ORDERS_DATASET_ORDER_PAYMENTS_DATASET_ORDER_ITEMS_DATASET_SQLTAG_DEST#v11451","session_name":"ML_Demo", "user_name":"admin"} -schema {"ORDER_ID":"string", "DELIVERY_DAYS_LOG":"float", "PAYMENT_LOG":"float", "predict":"float", "xpu":"int", "error":"string"} -tname test#123

# app -load kafka-base:1 -parser /sharedUDFs/rme:rme -args {"table_id":"ORDERS_DATASET_ORDER_PAYMENTS_DATASET_ORDER_ITEMS_DATASET_SQLTAG_DEST#v10331","session_name":"ML_Demo", "user_name":"admin"} -schema {"ORDER_ID":"string", "DELIVERY_DAYS_LOG":"float", "PAYMENT_LOG":"float", "predict":"float", "xpu":"int", "error":"string"} -tname test#123     
                
# ["ORDER_ID","PURCHASE","DELIVERED","DELIVERY_DAYS","PAYMENT","DELIVERY_DAYS_LOG","PAYMENT_LOG","CUSTOMER_NAME","RN","CID"]
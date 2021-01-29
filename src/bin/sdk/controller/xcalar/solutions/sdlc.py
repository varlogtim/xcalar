from xcalar.external.dataflow import Dataflow
from uuid import uuid1
import json
from structlog import get_logger
import re

logger = get_logger(__name__)

PARAM_PATTERN = re.compile('<.+>')


class SdlcManager:
    def __init__(self, dispatcher, vcsAdpater):
        self.dispatcher = dispatcher
        self.vcsAdapter = vcsAdpater
        self.xcalarClient = dispatcher.xcalarClient

    def checkout_shared_udfs(self, names, branchname, path):
        logger.debug(
            'Checking out shared UDF: {}, from Branch: {}, Path: {}'.format(
                names, branchname, path))
        try:
            udfs = {}
            for name in names:
                code = self.vcsAdapter.get(
                    branchname, '{}/sharedUDFs/{}.py'.format(path, name))
                logger.debug(
                    'Downloaded code for {}, now creating it'.format(name))
                udfs[name] = self.dispatcher.update_or_create_shared_udf(
                    name, code, unique_name=False)
            logger.debug('Checked out and created shared UDFs successfully')
            return udfs
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def checkout_udf(self, names, branchname, path):
        logger.debug('Checking out UDFs: {}, from Branch: {}, Path: {}'.format(
            names, branchname, path))
        try:
            udfs = {}
            for name in names:
                code = self.vcsAdapter.get(branchname, '{}/{}.py'.format(
                    path, name))
                logger.debug(
                    'Downloaded code for {}, now creating it'.format(name))
                try:
                    m = self.dispatcher.workbook.get_udf_module(name)
                    m.delete()
                except Exception as e:
                    logger.debug(f'Ignoring error: {str(e)}')
                udfs[name] = self.dispatcher.update_or_create_udf(
                    name, code, unique_name=False)
            logger.debug('Checked out and created UDFs successfully')
            return udfs
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def checkout_dataflow(self, branchname, path, df_name):
        logger.debug('Checking out dataflow from branch: {}, path: {}'.format(
            branchname, path))
        try:
            query = self.vcsAdapter.get(branchname,
                                        '{}/query_string.json'.format(path))
            optimized_query = self.vcsAdapter.get(
                branchname, '{}/optimized_query_string.json'.format(path))
            df = Dataflow.create_dataflow_from_query_string(
                self.dispatcher.xcalarClient,
                query_string=query,
                optimized_query_string=optimized_query,
                dataflow_name=f'{df_name}_{uuid1().__str__()}',
            )
            logger.debug('Created dataflow')
            udf_names = self.dispatcher.extract_udf_from_dataflow(
                df, check_if_exists=False)
            udf_names.discard('')
            if len(udf_names) > 0:
                self.checkout_udf(udf_names, branchname, path)
            logger.debug('Checked out dataflow successfully')
            return df
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def checkin_udfs(self, names, branchname, path, workbook_name=None):
        logger.debug(
            'Checking in UDFs: {}, branch: {}, path: {}, workbook_name: {}'.
            format(names, branchname, path, workbook_name))
        if workbook_name:
            workbook = self.xcalarClient.get_workbook(workbook_name)
        else:
            workbook = self.dispatcher.workbook
        try:
            udfs = []
            for name in names:
                udfs.append(workbook.get_udf_module(name))
            self.vcsAdapter.checkin_udfs([{
                'name': workbook.name,
                'udfs': udfs
            }], branchname, path)
            logger.debug('Checked in UDFs successfully')
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def checkin_shared_udfs(self, names, branchname, path):
        logger.debug('Checking in UDFs: {}, branch: {}, path: {}'.format(
            names, branchname, path))
        try:
            udfs = []
            for name in names:
                udfs.append(self.dispatcher.xcalarClient.get_udf_module(name))
            self.vcsAdapter.checkin_udfs([{
                'name': 'sharedUDFs',
                'udfs': udfs
            }], branchname, path)
            logger.debug('Checked in shared UDFs successfully')
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def checkin_all_dataflows(self,
                              names,
                              branchname,
                              path,
                              workbook_name=None):
        logger.debug(
            'Checking in dataflows {}, branch:{}, path: {}, workbook_name: {}'.
            format(names, branchname, path, workbook_name))
        workbook = (self.dispatcher.xcalarClient.get_workbook(workbook_name)
                    if workbook_name else self.dispatcher.workbook)
        try:
            # xdb = XDBridge(orchestrator=???)
            # xdb.updateSources(mode='params')
            all_dataflows = {}
            all_dataflows['target_dataflows'] = []
            all_dataflows['interactive_dataflows'] = {}
            udfs = []
            for name in names:
                (successful, errors) = self.validate_dataflow(
                    name, workbook_name=workbook_name)
                if not successful:
                    logger.warning(
                        f'One or more dataflows have a validation error, continuing checkin'
                    )
                    for title, error in errors.items():
                        logger.warning(
                            f'Validation failure in Dataflow: {title}: {error}'
                        )
                df = self.dispatcher.getDataflow(name, wbk_name=workbook_name)
                all_dataflows['target_dataflows'].append({
                    'dataflow_name': name,
                    'query_string': df[0],
                    'optimized_query_string': df[1],
                })
                all_dataflows[
                    'interactive_dataflows'] = self.get_interactive_dataflows(
                        name, workbook_name)

                udf_set = self.dispatcher.extract_udf_from_dataflow(df[1])
                udf_set.discard('')
                udfs.append({
                    'name': name,
                    'udfs': map(lambda n: workbook.get_udf_module(n), udf_set),
                })
            self.vcsAdapter.checkin_dataflows_and_udfs(all_dataflows, udfs,
                                                       branchname, path)
            logger.debug('Checked in dataflows successfully')
            return errors
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

    def validate_dataflow(self, dataflow_name, workbook_name=None, first=True):
        logger.info(
            f'Validating dataflow {dataflow_name} from workbook: {workbook_name}'
        )
        workbook = (self.xcalarClient.get_workbook(workbook_name)
                    if workbook_name else self.dispatcher.workbook)
        try:
            error_nodes = {}
            upstream_dataflows = []
            # Fetch all state from kvstore
            keys = workbook.kvstore.list(workbook._list_df_regex)
            if len(keys) == 0:
                error_message = f'No dataflows found in {workbook.name}'
                logger.error(error_message)
                return (False, {})

            is_found = False
            for key in keys:
                value = workbook.kvstore.lookup(key)
                kvs_json = json.loads(value)

                # Check whether this kvs entry is for the dataflow
                if kvs_json['name'] == dataflow_name:
                    is_found = True
                    # Check each node in the dag
                    for node in kvs_json['dag']['nodes']:
                        # logger.debug(f'Processing node: {node["title"]}')

                        # If a link in node is found
                        # 1) Check if it is configured or not
                        # 2) Check if it has one of 'dataflowId and linkOutName' OR sourceId
                        # Also add it to a list of dataflows to be processed recursively
                        # Check if link after execution is checked
                        if node['type'] == 'link in':
                            if not node['configured']:
                                logger.debug(f'Node is not configured')
                                error_nodes.update({
                                    f"{dataflow_name}/{node['title']}":
                                        'link in node is not configured'
                                })

                            if (node['input']['dataflowId']
                                    or node['input']['linkOutName']):
                                if node['input']['source']:
                                    error_message = f'Node has dataflowId or linkOutName configured, cannot have source'
                                    logger.debug(error_message)
                                    error_nodes.update({
                                        f"{dataflow_name}/{node['title']}":
                                            error_message
                                    })
                                elif ((node['input']['dataflowId']
                                       and node['input']['linkOutName'])):
                                    upstream_dataflows.append(
                                        json.loads(
                                            workbook.kvstore.lookup(
                                                node['input']['dataflowId']))
                                        ['name'])

                            elif (not node['input']['dataflowId']
                                  and not node['input']['linkOutName']
                                  and not node['input']['source']):
                                error_message = f'Node does not have (dataflowId and linkoutName) or source configured'
                                logger.debug(error_message)
                                error_nodes.update({
                                    f"{dataflow_name}/{node['title']}":
                                        error_message
                                })

                            if (dataflow_name == 'Input Data' and not bool(
                                    re.match(PARAM_PATTERN,
                                             node['input']['source']))):
                                error_message = f'Node has non param source'
                                logger.debug(error_message)
                                error_nodes.update({
                                    f"{dataflow_name}/{node['title']}":
                                        error_message
                                })

                        if node['type'] == 'link out':
                            if node['input'][
                                    'linkAfterExecution'] and not first:
                                error_message = f'Node has link after execution configured, please uncheck this on the node'
                                logger.debug(error_message)
                                error_nodes.update({
                                    f"{dataflow_name}/{node['title']}":
                                        error_message
                                })

            if not is_found:
                error_message = (
                    f'Dataflow {dataflow_name} not found in {workbook.name}')
                logger.error(error_message)
                return (False, {})
            # Now handle upstream dataflows
            for df in set(upstream_dataflows):
                (successful, errors) = self.validate_dataflow(
                    df, workbook_name=workbook_name, first=False)
                if not successful:
                    error_nodes.update(errors)

            return (True, {}) if len(error_nodes) == 0 else (False,
                                                             error_nodes)
        except Exception as e:
            logger.error(f'Failed to load dataflow: {str(e)}')
            raise e

    def get_interactive_dataflows(self, dataflow_name, workbook_name=None):
        workbook = (self.dispatcher.xcalarClient.get_workbook(workbook_name)
                    if workbook_name else self.dispatcher.workbook)
        keys = workbook.kvstore.list(workbook._list_df_regex)
        if len(keys) == 0:
            return []
        ret = []
        upstream_dataflows = []
        for key in keys:
            value = workbook.kvstore.lookup(key)
            kvs_json = json.loads(value)

            # Check whether this kvs entry is for the dataflow
            if kvs_json['name'] == dataflow_name:
                ret.append({
                    'dataflow_name': dataflow_name,
                    'kvs_json': kvs_json
                })
                for node in kvs_json['dag']['nodes']:
                    if node['type'] == 'link in':
                        if node['input']['dataflowId'] and node['input'][
                                'linkOutName']:
                            upstream_dataflows.append(
                                json.loads(
                                    workbook.kvstore.lookup(
                                        node['input']['dataflowId']))['name'])

        for df in set(upstream_dataflows):
            upstream_ret = self.get_interactive_dataflows(df, workbook_name)
            d = {x['dataflow_name']: x for x in upstream_ret}
            filtered = list(d.values())
            ret += filtered
        return ret

    def checkout_interactive_dataflows(self,
                                       branchname,
                                       root_path,
                                       universe,
                                       workbook_name=None):
        workbook = (self.dispatcher.xcalarClient.get_workbook(workbook_name)
                    if workbook_name else self.dispatcher.workbook)
        logger.debug(
            'Checking out all dataflows in branch: {}'.format(branchname))
        try:
            dataflow_paths = [
                dataflow['path'] for dataflow in self.vcsAdapter.listTree(
                    branchname, root_path)
            ]
            for path in dataflow_paths:
                logger.info(f'Beginning to check out {path}')
                kvsvalue = self.vcsAdapter.get(branchname,
                                               '{}/kvsvalue.json'.format(path))
                kvs_json = json.loads(kvsvalue)
                kvstore = workbook.kvstore
                kvstore.add_or_replace(kvs_json['id'], kvsvalue, True)

                if (self.vcsAdapter.exists(
                        branchname, f'{path}/optimized_query_string.json')):
                    optimized_query = self.vcsAdapter.get(
                        branchname,
                        '{}/optimized_query_string.json'.format(path))
                    udf_names = self.dispatcher.extract_udf_from_dataflow(
                        optimized_query, check_if_exists=False)
                    udf_names.discard('')
                    if len(udf_names) > 0:
                        self.checkout_udf(udf_names, branchname, path)
                logger.info('Checked out dataflow ' + kvs_json['name'])
        except Exception as e:
            logger.error('Error: {}'.format(e))
            logger.debug(str(e))
            raise e

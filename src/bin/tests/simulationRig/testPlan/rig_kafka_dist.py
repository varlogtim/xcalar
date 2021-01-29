from itertools import cycle


class Distributer:
    def __init__(self, xpu_count, distributer_props):

        self.parse_args = {'maxIter': 20, 'batch_id': 0, 'metadataInfo': {}}
        self.xpu_count = xpu_count
        self.server_topic_map = {
            'localhost:9092': {
                'JD_PositionsSTB': [{
                    'topic': 'JD_PositionsSTB',
                    'partition': 0
                }],
                'JD_Taxlots_Cost': [{
                    'topic': 'JD_Taxlots_Cost',
                    'partition': 0
                }],
                'JD_PWM_Prices': [{
                    'topic': 'JD_PWM_Prices',
                    'partition': 0
                }],
                'JD_PWM_Unit_FI_Accruals': [{
                    'topic': 'JD_PWM_Unit_FI_Accruals',
                    'partition': 0
                }],
                'JD_BDADailyBalAccrlTB': [{
                    'topic': 'JD_BDADailyBalAccrlTB',
                    'partition': 0
                }],
                'JD_GSEntlInflightTB': [{
                    'topic': 'JD_GSEntlInflightTB',
                    'partition': 0
                }],
                'JD_GSEntlPosnTB': [{
                    'topic': 'JD_GSEntlPosnTB',
                    'partition': 0
                }]
            }
        }
        self.kafka_cluster_props = {
            'topic_info': {
                'JD_PositionsSTB': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_Taxlots_Cost': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_PWM_Prices': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_PWM_Unit_FI_Accruals': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_BDADailyBalAccrlTB': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_GSEntlInflightTB': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                },
                'JD_GSEntlPosnTB': {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'NEARTIME_CG',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                }
            }
        }

    def _math_formula(self, elem_to_weight_map, number_of_cores):
        per_element_weight = number_of_cores / sum(elem_to_weight_map.values())
        elem_to_num_core_map = {}
        for elem, weight in elem_to_weight_map.items():
            elem_to_num_core_map[elem] = round(weight * per_element_weight)
        return elem_to_num_core_map

    def assign_xpu_to_topics(self, topic_partition_list_map):
        topic_to_xpu_number_map = {}
        # first assign 1 xpu to each element
        topic_to_num_partitions = {}
        for topic_name, topic_partition_list in topic_partition_list_map.items(
        ):
            topic_to_num_partitions[topic_name] = len(topic_partition_list)

        # whatever remains, assing using math formula
        remaining_number_of_xpus = self.xpu_count - len(
            topic_partition_list_map)
        topic_to_num_cores = self._math_formula(topic_to_num_partitions,
                                                remaining_number_of_xpus)
        final_topic_to_num_cores = {
            k: v + 1
            for (k, v) in topic_to_num_cores.items()
        }
        xpu_assign_idx = 0
        for topic_name in topic_partition_list_map:
            num_cores_assigned = final_topic_to_num_cores[topic_name]
            end_idx = xpu_assign_idx + num_cores_assigned
            topic_to_xpu_number_map[topic_name] = range(
                xpu_assign_idx, end_idx)
            xpu_assign_idx = end_idx

        # topic -> topic partitions

        topic_partition_details = topic_partition_list_map
        # create xpu -> partition list
        xpu_partition_map = {}
        for topic_name, xpu_assignment in topic_to_xpu_number_map.items():
            topic_partitions_to_assign = topic_partition_details[topic_name]
            for i in xpu_assignment:
                xpu_partition_map[i] = []
            for i in zip(topic_partitions_to_assign, cycle(xpu_assignment)):
                xpu_partition_map[i[1]].append(i[0])
        return xpu_partition_map

    def _assign_xpu(self):
        if len(
                self.server_topic_map.keys()
        ) == 1:    # we have to connect to only 1 server. We should split all xpus among topics
            return self.assign_xpu_to_topics(
                next(iter(self.server_topic_map.values())))
        else:
            topic_partition_uber_map = {}
            for server_topic_partition_map in self.server_topic_map.values():
                topic_partition_uber_map.update(server_topic_partition_map)
            return self.assign_xpu_to_topics(topic_partition_uber_map)

    def distribute(self):
        self.server_topic_map = dict(self.server_topic_map)
        xpu_topic_partition_map = self._assign_xpu()
        for key in xpu_topic_partition_map:
            if len(xpu_topic_partition_map[key]) == 0:
                continue
            meta = {
                'kafka_props':
                    self.kafka_cluster_props['topic_info']
                    [xpu_topic_partition_map[key][0]['topic']],
                'topic':
                    xpu_topic_partition_map[key][0]['topic'],
                'partitionInfo': {}
            }

            for topic_partition in xpu_topic_partition_map[key]:
                meta['partitionInfo'][str(topic_partition['partition'])] = {
                    'min': 0,
                    'max': 9999999
                }
                self.parse_args['metadataInfo'][str(key)] = meta
        return self.parse_args

    def updateOffsets(self, _batch_id, offsets):
        offsets = list(offsets)
        self.parse_args['batch_id'] = _batch_id
        for key in self.parse_args['metadataInfo']:
            topic = self.parse_args['metadataInfo'][key]['topic']
            pinfo = self.parse_args['metadataInfo'][key]['partitionInfo']

            partition = list(pinfo.keys())[0]
            # loop offsets
            for record in offsets:
                if ((record['topic'] == topic)
                        and (str(record['partition']) == str(partition))):
                    self.parse_args['metadataInfo'][key]['partitionInfo'][str(
                        partition)]['min'] = record['max_offset'] + 1
        return self.parse_args

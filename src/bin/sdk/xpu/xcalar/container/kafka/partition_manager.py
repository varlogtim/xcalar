from xcalar.container.kafka.custom_logger import CustomLogger
import logging


class PartitionManager():
    def __init__(self, logger: CustomLogger):
        self.logger = logger

    def xpu_partition_map(self, num_nodes, xpu_start_ids: list,
                          xpus_per_node: list, partitions: list):
        self.logger.log(
            logging.INFO,
            f'Generating xpu_partition_map with num_nodes:{num_nodes}, xpu_start_ids:{xpu_start_ids}, xpus_per_node: {xpus_per_node}, partitions: {partitions}'
        )
        xpu_partition_map = {}
        step_size = sum(xpus_per_node) * num_nodes
        # In rare cases, when you have more partitions than XPUs then split partitions by len(xpus) and cycle through it
        partition_chunks = [
            partitions[i:i + step_size]
            for i in range(0, len(partitions), step_size)
        ]
        for chunk in partition_chunks:
            for node in range(0, num_nodes):
                start = xpu_start_ids[node]
                end = start + xpus_per_node[node]
                for i in range(start, end):
                    partition_index = i - xpu_start_ids[0]
                    self.logger.log(
                        logging.DEBUG,
                        f'start:{start}, end: {end}, i: {i}, xpu_start_ids: {xpu_start_ids[0]}, partition_index:{partition_index}, chunk: {chunk}'
                    )
                    if partition_index >= len(chunk):
                        continue
                    if i not in xpu_partition_map:
                        xpu_partition_map[i] = []
                    xpu_partition_map[i].append(chunk[partition_index])
        self.logger.log(logging.INFO,
                        f'Generated xpu_partition_map: {xpu_partition_map}')
        return xpu_partition_map

from confluent_kafka import Consumer


class KafkaMetadata():
    def __init__(self, consumer=None, consumer_props=None):
        if consumer is None and consumer_props is None:
            raise Exception(
                'Atleast one of consumer or consumer_props must be given')
        if consumer is None:
            self.consumer = Consumer(consumer_props)
        else:
            self.consumer = consumer

    def get_partitions(self, topic):
        consumer = None
        try:
            cluster_metadata = self.consumer.list_topics(topic)
            topic_metadata = cluster_metadata.topics[topic]
            return list(topic_metadata.partitions.keys())
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass

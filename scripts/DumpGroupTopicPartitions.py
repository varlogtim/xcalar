"""
Using this tool, you can print or dump some Kafka statistics like
topics, partitions in the topic, total and consumed events (last
one as applied to a specified group). Topics can be created
explicitly by making a request to the zookeeper to add a new topic,
and the number of partitions are usually specified at that time.
Topics can be created implicitly, when a producer sends a message
for the first time on the topic, and the number of partitions are
defaulted to the default partitions in the kafka server.properties
file. In both cases, the added topics are reflected in the output
of this tool. When a Kafka producer sends events to Kafka, this tool 
should reflect the increase in total events.  When events expire 
or are removed from Kafka, the tool should reflect the decrease 
in total events. The consumed events change for a
specific group for a specific topic, when a consumer runs using the 
specified consumer group. This counters are updated based on which
partition the event was assigned to.

Usage: DumpGroupTopicPartitions.py [options]

Options:
  -h, --help            show this help message and exit
  -t TOPIC, --topic=TOPIC
                        topic
  -H HOSTS, --hosts=HOSTS
                        hosts
  -g GROUP, --group=GROUP
                        group
"""
import optparse
from kafka import KafkaConsumer, TopicPartition, KafkaClient

def dumpPartitionOffsets(hosts, topic, group):
    consumer = KafkaConsumer(bootstrap_servers=hosts)
    groupConsumer = KafkaConsumer(bootstrap_servers=hosts, group_id=group)
    client = KafkaClient(hosts)
    if topic:
        topics = [topic]
    else:
        topics = consumer.topics()
    grandTotal = 0
    grandPending = 0
    for topic in topics:
        partitionIds = client.get_partition_ids_for_topic(topic)
        topicPartitions = []
        for ii in partitionIds:
             topicPartitions.append(TopicPartition(topic, ii))
        consumer.assign(topicPartitions)
        groupConsumer.assign(topicPartitions)
        ## get the offset of the next record for this partition on a give topic
        totalSize = 0
        totalPending = 0
        print("\nTopic: {}".format(topic))
        for ii in partitionIds:
            total = consumer.position(topicPartitions[ii])
            consumed = groupConsumer.committed(topicPartitions[ii]) 
            if consumed is None:
                consumed = 0
            if total == consumed:
                status = "DONE"
            else:
                status = str(total-consumed)
            print(('Partition: {}, Total: {}, Group: {}, Consumed: {}, Pending: {}'.format(ii, total, group, consumed, status)))
            totalSize += total
            totalPending += (total-consumed)
        if totalSize == 0:
            perc = 0
        else:
            perc = totalPending*100/float(totalSize)
        print("Total events: {0}, Pending: {1}, %Pending: {2:.2f}%".format(totalSize, totalPending, perc))
        grandTotal += totalSize
        grandPending += totalPending
    if grandTotal == 0:
        perc = 0
    else:
        perf = grandPending*100/float(grandTotal)
    print("\n\nGrand Total events: {0}, Pending: {1}, %Pending: {2:.2f}%".format(grandTotal, grandPending, perc))

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-t', '--topic', action="store", dest="topic", help="topic", default=None)
    parser.add_option('-H', '--hosts', action="store", dest="hosts", help="hosts", default="localhost:9092")
    parser.add_option('-g', '--group', action="store", dest="group", help="group", default="demo")
    options, args = parser.parse_args()
    dumpPartitionOffsets(options.hosts, options.topic, options.group)

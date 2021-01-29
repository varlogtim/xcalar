from confluent_kafka import Producer, Consumer
from datetime import datetime
import json
import sys

delivered_records = 0


def main(argv):
    topic = argv[0]
    broker = argv[1]

    print(f'Testing topic: {topic}, broker: {broker}')

    # Create Producer instance
    producer = Producer({'bootstrap.servers': broker})
    topics = producer.list_topics()
    print(f'Existing Topics: {list(topics.topics.keys())}')

    def acked(err, msg):
        global delivered_records
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}".
                  format(msg.topic(), msg.partition(), msg.offset()))

    for n in range(3):
        record_key = "test"
        now = datetime.now()
        record_value = json.dumps({
            'count': n,
            'time': now.strftime("%H:%M:%S")
        })
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(
            topic, key=record_key, value=record_value, on_delivery=acked, partition=0)
        producer.poll(0)

    producer.flush(timeout=5)

    print("{} messages were produced to topic {}!".format(
        delivered_records, topic))
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'XXX1',
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'
    })

    # Subscribe to topic
    def print_assignment(consumer, partitions):
        print(f'Assignment:{partitions}')

    consumer.subscribe([topic], on_assign=print_assignment)

    # Process messages
    total_count = 0
    no_message_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if no_message_count > 10:
                    print("No more messages. Exiting")
                    break
                else:
                    print("Waiting for message or event/error in poll()")
                    no_message_count += 1
                    continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}".format(
                    record_key, record_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


if __name__ == "__main__":
    if len(sys.argv) == 3:
        main(sys.argv[1:])
    else:
        main([
            # 'test1', 'qa.xcalar.rocks:9092'
            'test1', 'ec2-54-184-255-230.us-west-2.compute.amazonaws.com:9092'
            # 'test1', '54.244.96.153:9092'
        ])

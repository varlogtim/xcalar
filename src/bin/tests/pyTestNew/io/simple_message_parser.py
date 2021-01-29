from datetime import datetime
import xcalar.container.context as ctx
import time
logger = ctx.get_logger()


def get_schema(topic):
    return {
        'topic': 'string',
        'partition': 'int',
        'key': 'string',
        'headers': 'string',
        'offset': 'int',
        'ts_nanos': 'float',
        'size': 'int',
        'msg': 'string',
        'error': 'string',
        'index': 'int'
    }


def parse_kafka_stream(kafka_stream, context):
    for msg in kafka_stream:
        result = {
            'msg':
                str(msg.value()),
            'ts_nanos':
                time.time() * 1000 * 1000 * 1000,
            'size':
                len(msg),
            'headers':
                msg.headers(),
            'error':
                msg.error(),
            'key':
                str(msg.key()),
            'topic':
                msg.topic(),
            'offset':
                msg.offset(),
            'partition':
                msg.partition(),
            'index':
                int(f'{int(datetime.utcnow().timestamp() * 1000 * 1000)}{context["xpu_id"]:03}'
                    )
        }
        yield result

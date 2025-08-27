from pykafka import KafkaClient
import tensorwatch as tw
from pykafka.common import OffsetType
import collections
from datetime import datetime
import threading

# from .watcher import observe as tw

#imported parsers
import json
import pickle
import avro.schema
# import thrift
# # import ParseFromString
# from thrift.protocol import TBinaryProtocol
# from thrift.transport import TTransport
# import xml.etree.ElementTree as ET

def basicKafka(hosts:str=None,topic:str=None,parsetype:str=None,parser_extra:str=None,queue_length:int=None):
    #threading Kafka so that we can make it run the message loop on background
    thread = threading.Thread(target=consumer(hosts=hosts,topic=topic))
    print("test")
    return

#trying to add deferent libraries for deserializing the kafka messages
def myparser(message,parsetype=None,parser_extra=None):
    if parsetype==None or parsetype.lower()=='json' :
        return json.loads(message)
    elif parsetype==None or parsetype.lower()=='pickle' :
        return pickle.loads(message)
    # elif parsetype==None or parsetype.lower()=='thrift' :
    #     # transportIn = TTransport.TMemoryBuffer(message)
    #     # return TBinaryProtocol.TBinaryProtocol(transportIn)
    #     TDeserializer.deserialize(parser_extra, data)
    # elif parsetype==None or parsetype.lower()=='xml' :
    #     return pickle.loads(message)
    # elif parsetype==None or parsetype.lower()=='protobuf' :
    #     # s = str(message, 'ascii')
    #     s = str(message, 'utf-8')
    #     return ParseFromString(s)
        # import base64
        # s = base64.b64decode(data).decode('utf-8')
        # message.ParseFromString(s)
        # transportIn = TTransport.TMemoryBuffer(message)
        # return TBinaryProtocol.TBinaryProtocol(transportIn)
    elif parsetype=='' or parsetype.lower()=='avro' :
        return avro.schema.parse(message)
    return 'error:unkown type of parsing'

def consumer(hosts:str=None,topic:str=None,parsetype:str=None,parser_extra:str=None,queue_length:int=None):
    #the kafka setup
    if hosts==None:
        client = KafkaClient(hosts="127.0.0.1:9093")
    else:
        client = KafkaClient(hosts=hosts)
    topic = client.topics[topic]
    consumer = topic.get_simple_consumer(    
        auto_offset_reset=OffsetType.LATEST,
        reset_offset_on_start=True,
        fetch_wait_max_ms =50,
        )
    w = tw.Watcher()
    #queue_length is the maximum messages that will be kept in memory
    if queue_length==None:
        data = collections.deque(maxlen=500)
    else:
        data = collections.deque(maxlen=queue_length)
    for message in consumer:
        timen=datetime.now()
        if message is not None:
            temp=myparser(message.value,parsetype)
            temp["Date"]= datetime.strptime(temp["Date"], '%d/%b/%Y:%H:%M:%S')
            temp["recDate"]=datetime.now()
            data.append(temp)
            w.observe(data=list(collections.deque(data)))#,counterGet=counterGet,counterPost=counterPost,Date=Date,request=request,extra=extra,timen=timen,recDate=recDate)
    return
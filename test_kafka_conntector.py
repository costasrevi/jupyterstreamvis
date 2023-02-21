# from tensorwatch.watcher_client import WatcherClient
# from tensorwatch.kafka_contector import kafka_contector #  as kc
import tensorwatch as tw
tw.kafka_connector(topic='my.test')
import warnings
import logging

warnings.filterwarnings("ignore", category=UserWarning, module="pykafka")
warnings.filterwarnings("ignore", category=UserWarning, module='pkg_resources')

# Suppress "No partitions assigned" warnings from pykafka.balancedconsumer, which can be noisy during rebalancing.
logging.getLogger('pykafka.balancedconsumer').setLevel(logging.ERROR)

from .twapi import twapi
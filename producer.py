#!/usr/bin/env python
import threading, logging, time, sys
import traceback

import kafka
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class Producer():

    def run(self):
        try:
            client = KafkaClient("pp-logkafka-01.qasql.opentable.com:9092,pp-logkafka-02.qasql.opentable.com:9092,pp-logkafka-03.qasql.opentable.com:9092")

            producer = SimpleProducer(client, "")
            producer.send_messages("lifecycle_events","test")

        except:
            tb = traceback.format_exc()
        finally:
            print tb

if __name__ == "__main__":
    Producer().run()

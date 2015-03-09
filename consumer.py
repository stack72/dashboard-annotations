#!/usr/bin/env python
import threading, logging, time, sys
import traceback

import kafka
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer

class Consumer():

    def run(self):
        try:
            kafka = KafkaConsumer("lifecycle_events", metadata_broker_list=["pp-logkafka-01.qasql.opentable.com:9092","pp-logkafka-02.qasql.opentable.com:9092","pp-logkafka-03.qasql.opentable.com:9092"])
            while True:
              m =  kafka.next()
              print m
              kafka.task_done(m)
              kafka.commit()
              print "\n\n"

        except:
            tb = traceback.format_exc()
        finally:
            print tb

if __name__ == "__main__":
    Consumer().run()

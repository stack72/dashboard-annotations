#!/usr/bin/env python
import threading, logging, time, sys

import kafka
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer

class Consumer():

    def run(self):
        try:
            kafka = KafkaConsumer("logstash", metadata_broker_list=["pp-logkafka-01.qasql.opentable.com:9092","pp-logkafka-02.qasql.opentable.com:9092","pp-logkafka-03.qasql.opentable.com:9092"],group_id="annotations",)

            while True:
              m =  kafka.next()
              print m
              kafka.task_done(m)
              kafka.commit()
              print "\n\n"

        except:
            print "Fail"
            print "Unexpected error:", sys.exc_info()[0]

if __name__ == "__main__":
    Consumer().run()

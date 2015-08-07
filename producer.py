#!/usr/bin/env python
import threading, logging, time, sys, getopt
import traceback
import datetime

from kafka import SimpleProducer, KafkaClient

class Producer():

    def __init__(self, nodes=None, application=None, depType=None, message=None):
        global kafkaNodes
        kafkaNodes = nodes
        global applicationName
        applicationName = application
        global deploymentType
        deploymentType = depType
        global deploymentMessage
        deploymentMessage = message

    def run(self):
        current_date = datetime.datetime.now()

        try:
            kafka = KafkaClient(kafkaNodes)

            producer = SimpleProducer(kafka, async=True)
            producer.send_messages('logs',applicationName,deploymentType,deploymentMessage,current_date.isoformat())

        except:
            print traceback.format_exc()

if __name__ == "__main__":
    kafkaNodes = None;
    applicationName = None;
    deploymentType = None;
    deploymentMessage = None;

    myopts, args = getopt.getopt(sys.argv[1:],"n:a:d:m:")
    ###############################
    # o == option
    # a == argument passed to the o
    ###############################
    for o, a in myopts:
        if o == '-n':
            kafkaNodes = a
        elif o == '-a':
            applicationName = a
        elif o == '-d':
            deploymentType = a
        elif o == '-m':
            deploymentMessage = a
        else:
            print("Usage: %s -n kafka-nodes-list -a applicationName -d deploymentType -m deploymentMessage" % sys.argv[0])
    producer = Producer(kafkaNodes, applicationName, deploymentType, deploymentMessage).run()

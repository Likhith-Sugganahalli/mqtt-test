#!/usr/bin/env python3

from multiprocessing.dummy import Pool
import time
import requests
import sys
import paho.mqtt.client as mqtt
import logging
import yaml


class MqttTest():
	def __init__(self):

		logging.basicConfig(filename='/home/pi/bg-processes/mqtt-test/mqtt-test.log', filemode='w', format='%(asctime)s; %(name)s; %(levelname)s; %(message)s')
		logging.root.setLevel(logging.NOTSET)
		logging.info('logging started')


		try:
			self.config = yaml.safe_load(open("/home/pi/bg-processes/mqtt-test/config.yaml"))
			ret = self.mqtt_subscribe_thread_start(self.iot_func_callback_sub,self.config['arg_broker_url'],self.config['arg_broker_port'],self.config['arg_mqtt_sub_topic'],self.config['arg_mqtt_qos'])
			if ret != 0:
				raise Exception('Error with subscribe thread exception caught{}'.format(ret))

		except Exception as e:
			logging.error(e)


	def iot_func_callback_sub(self,client, userdata, message):
		#print("message received {} topic:{}   retain_flag:{}".format(message.payload.decode("utf-8"),message.topic,message.retain))
		messageReceived = message.payload.decode("utf-8")
		logging.info("message received {} topic:{}   retain_flag:{}".format(messageReceived,message.topic,message.retain))
		if messageReceived == 'test':
			logging.info('received test signal, sending confirmation back')
			self.mqtt_publish(self.config['arg_broker_url'],self.config['arg_broker_port'],self.config['arg_mqtt_pub_topic'],'0',self.config['arg_mqtt_qos'])
		else:
			logging.info('received wrong message on test topic {}'.format(messageReceived))

	def mqtt_subscribe_thread_start(self,arg_callback_func, arg_broker_url, arg_broker_port, arg_mqtt_topic, arg_mqtt_qos):
		try:
			mqtt_client = mqtt.Client('mqtt_test_sub')
			mqtt_client.on_message = arg_callback_func
			mqtt_client.connect(arg_broker_url, arg_broker_port)
			mqtt_client.subscribe(arg_mqtt_topic, arg_mqtt_qos)
			time.sleep(1) # wait
			mqtt_client.loop_forever() # starts a blocking infinite loop
			#mqtt_client.loop_start()    # starts a new thread
			return 0
		except Exception as e:
			return e

	def mqtt_publish(self,arg_broker_url, arg_broker_port, arg_mqtt_topic, arg_mqtt_message, arg_mqtt_qos):
		try:
			mqtt_client = mqtt.Client("mqtt_test_pub")
			mqtt_client.connect(arg_broker_url, int(arg_broker_port))
			mqtt_client.loop_start()

			logging.info("Publishing message to topic {}".format(arg_mqtt_topic))
			mqtt_client.publish(arg_mqtt_topic, arg_mqtt_message, arg_mqtt_qos)
			time.sleep(0.1) # wait

			mqtt_client.loop_stop() #stop the loop
			return 0
		except Exception as e:
			return e


if __name__ == '__main__':
	testObj = MqttTest()

from __future__ import absolute_import, division, print_function

import re

from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE
import json
import os


class Motioncor2Runner(CommonService):
	'''A zocalo service for running Scipion'''

	# Human readable service name
	_service_name = "Motioncor2 Runner"

	# Logger name
	_logger_name = 'motioncor2.zocalo.services.runner'

	def initializing(self):
		'''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
		queue_name = "motioncor2_runner"
		self.log.info("queue that is being listended to is %s" % queue_name)
		workflows.recipe.wrap_subscribe(self._transport, queue_name,
										self.run_motioncor2, acknowledgement=True, log_extender=self.extend_log,
										allow_non_recipe_messages=True)

	def run_motioncor2(self, rw, header, message):
		self.log.info("Starting running Motioncor2")

		# command = 'echo %s' % str(rw.recipe)  # (' '.join(rw.recipe_step['parameters']['arguments']))
		# # (rw.recipe_step['parameters']['cwd'], ' '.join(rw.recipe_step['parameters']['arguments']))

		import subprocess
		from subprocess import Popen

		# command = 'cd %s;module load EM/MotionCor2/1.1.0; MotionCor2 %s' % (
		print("this is rw")
		print(rw)
		print("this is rw_recipie_step")
		print(rw.recipe_step)
		print("this is rw_recipie_step2")
		print(rw.recipe[rw.recipe_pointer+1])
		print("Header")
		print(header)


		



		cmd = ' '.join(rw.recipe_step['parameters'])

		command_to_run = 'module load scipion/release-1.2.1-zo; %s'%cmd
		print('command to run:')
		print(command_to_run)

		mc2_command = Popen(command_to_run, shell=True)
		job_id = mc2_command.pid
		rw.recipe[rw.recipe_pointer+1]['parameters'] = job_id



		# string_path = os.path.dirname(os.path.join(rw.recipe_step['parameters'][4],rw.recipe_step['parameters'][5]))

		# string_id = rw.recipe_step['parameters'][1].split(' ')[-1]
	

		# js = {'task_id': string_id,'job_id': job_id }
		# json_op = os.path.join(string_path,'jobid_mapper.json') 
		# with open(json_op,'w+') as json_op_file:
		# 	json.dump(js,json_op_file)





		# mc2_command.wait() # OR  timeout based on transient queue?

		self.log.info("Finish running Motioncor2")
		self.log.info("All is good")
		msg_id = header['message-id']
		sub_id = header['subscription']
		print('MSG_ID:%s' % msg_id)
		print('Sub:%s' % sub_id)
		print('header:%s' % header)
		# Mark's change to Motioncorr to check if all okay then acknowledge
        #if (all_ok()):
		rw.transport.ack(header)
		rw.send([])

		# self.log.info("Service complete")
		# self._shutdown()

		#TODO: shutdown the consumer timeout based on the last protocol name 

		# rw.transport.ack(header)
		# rw.send([])
		# print('send back')

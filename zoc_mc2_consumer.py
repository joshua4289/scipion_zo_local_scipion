from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen
import json
import os, re

# Active MQ Scipion Consumer started as gda2

class MotionCor2Runner(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "MotionCor2_runner"

    # Logger name
    _logger_name = 'MotionCor2.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        queue_name = "MotionCor2_runner"
        self.log.info("queue that is being listended to is %s" % queue_name)
        workflows.recipe.wrap_subscribe(self._transport, queue_name,
                                        self.run_MotionCor2, acknowledgement=True, log_extender=self.extend_log,
                                        allow_non_recipe_messages=True)

    def run_MotionCor2(self, rw, header, message):

        self.log.info("Start running MotionCor2 Zocalo")

        import subprocess
        from subprocess import Popen

        # get the parameters

        session = rw.recipe_step['parameters']
        self.log.info(session)
        arguments = session['arguments']
        scipion_dir = session['cwd']

        self.log.info("Scipion Dir is %s" % scipion_dir)

        cmd = ('source /etc/profile.d/modules.sh;'
               'module load EM/MotionCor2/1.1.0;'
               'MotionCor2 '
               ) 

        cmd += ' '.join(arguments)

        self.log.info(cmd)
   
        p1 = Popen(cmd + ' '.join(arguments), cwd=scipion_dir,shell=True)
        out_project_cmd, err_project_cmd = p1.communicate()

        p1.wait()

        #print("THESE ARE THE ERROR MESSAGES ")
        #print(err_project_cmd)

        self.log.info("Finish running MotionCor2 Zocalo")
        rw.transport.ack(header)
        rw.send([])

    
    def shutdown_consumer(self):
        ''' Shutdown Consumer based on the timeout mentioned in the Import step of workflow '''

        pass


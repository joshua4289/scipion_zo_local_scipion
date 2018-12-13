from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen
import json
import os, re

# Active MQ Scipion Consumer started as gda2

class Relion2DRunner(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Relion_refine_runner"

    # Logger name
    _logger_name = 'relion.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        queue_name = "Relion_refine_runner"
        self.log.info("queue that is being listended to is %s" % queue_name)
        workflows.recipe.wrap_subscribe(self._transport, queue_name,
                                        self.run_relion_2d, acknowledgement=True, log_extender=self.extend_log,
                                        allow_non_recipe_messages=True)

    def run_relion_2d(self, rw, header, message):

        self.log.info("Start running relion class 2d Zocalo")

        import subprocess
        from subprocess import Popen

        # get the parameters

        session = rw.recipe_step['parameters']
        self.log.info(session)
        arguments = session['arguments']
        scipion_dir = session['cwd']

        self.log.info("Scipion Dir is %s" % scipion_dir)

        # modify the GPU flag to be the correct GPU for this consumer
        # gpu_index = arguments.index('--gpu') + 1
        # arguments[gpu_index] = "999"  #os.getenv('SGE_HGR_gpu', 'GPU1')[3]

        self.log.info("Arguments are '%s'" % ' '.join(arguments))

        cmd = ('source /etc/profile.d/modules.sh;'
               'module load EM/relion; relion_refine '
               ) 

        cmd += ' '.join(arguments)

        self.log.info(cmd)
   
        p1 = Popen(cmd ,cwd=scipion_dir,shell=True)
                                           #+ ' '.join(arguments), cwd=scipion_dir,shell=True)
        out_project_cmd, err_project_cmd = p1.communicate()

        p1.wait()
        print ("SCIPION WORK DIR IS  %s" %(scipion_dir))
        print("THESE ARE THE ERROR MESSAGES")
        print(err_project_cmd)


        # p2 = Popen('touch done.tmp' ,cwd=scipion_dir,shell=True)
        # p2.wait()

        self.log.info("Finish running relion 2d Zocalo")
        rw.transport.ack(header)
        rw.send([])

    
    def shutdown_consumer(self):
        ''' Shutdown Consumer based on the timeout mentioned in the Import step of workflow '''

        pass


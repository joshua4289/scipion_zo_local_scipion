from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen
import json
import os, re

# Active MQ Scipion Consumer started as gda2

class GautomatchRunner(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Gautomatch_runner"

    # Logger name
    _logger_name = 'Gautomatch.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        queue_name = "Gautomatch_runner"
        self.log.info("queue that is being listended to is %s" % queue_name)
        workflows.recipe.wrap_subscribe(self._transport, queue_name,
                                        self.run_Gautomatch, acknowledgement=True, log_extender=self.extend_log,
                                        allow_non_recipe_messages=True)

    def run_Gautomatch(self, rw, header, message):

        self.log.info("Start running Gautomatch Zocalo")

        from subprocess import Popen

        # get the parameters
        session = rw.recipe_step['parameters']

        self.log.info(session)
        arguments = session['arguments']
        scipion_dir = session['cwd']

        self.log.info("Scipion Dir is %s" % scipion_dir)
        self.log.info("Arguments are '%s'" % ' '.join(arguments))

        # Compose the command and the environment
        cmd = ('source /etc/profile.d/modules.sh;'
               'module load EM/Gautomatch;'
               'Gautomatch '
               )
        cmd += ' '.join(arguments)

        self.log.info(cmd)

        # Run the command
        from subprocess import PIPE
        p1 = Popen(cmd ,cwd=scipion_dir,shell=True)

        #out_project_cmd, err_project_cmd = p1.communicate()
        #print(out_project_cmd)

        # Read output and feedback ...but Gautomatch does not do this
        # while p1.poll() is None: #True or p1.returncode != 0
        #     print(p1.stdout.readline())
        #
        # t_queue = (rw.recipe[rw.recipe_pointer + 1]['queue'])

        p1.wait()
        print ("SCIPION WORK DIR IS  %s" %(scipion_dir))
        print("THESE ARE THE ERROR MESSAGES")
        #print(err_project_cmd)


        # p2 = Popen('touch done.tmp' ,cwd=scipion_dir,shell=True)
        # p2.wait()

        self.log.info("Finish running Gautomatch Zocalo")
        print(header)
        rw.transport.ack(header)
        #rw.send([])

            # queue_name = str(t.Gautomatch_runner.
            # self.log.info("queue that is being listended to is %s" % queue_name)
            # workflows.recipe.wrap_subscribe(self._transport, queue_name,
            #                                 self.run_Gautomatch, acknowledgement=True, log_extender=self.extend_log,
            #                                 allow_non_recipe_messages=True)

    def shutdown_consumer(self):
        ''' Shutdown Consumer based on the timeout mentioned in the Import step of workflow '''

        pass


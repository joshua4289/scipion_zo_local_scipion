from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe

 

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

        command = 'echo %s' %str(rw.recipe)#(' '.join(rw.recipe_step['parameters']['arguments']))
        # (rw.recipe_step['parameters']['cwd'], ' '.join(rw.recipe_step['parameters']['arguments']))



        import subprocess
        from subprocess import Popen

        # command = 'cd %s;module load EM/MotionCor2/1.1.0; MotionCor2 %s' % (
        print ("this is rw") 
        print (rw)
        print ("this is rw_recipie_step") 
        print (rw.recipe_step)
        cmd = ' '.join(rw.recipe_step['parameters'])

  

        # command = ' '.join(rw.recipe_step['parameters']['arguments'])
        # #
        # # # rw.recipe_step['parameters']['cwd'], ' '.join(rw.recipe_step['parameters']['arguments']))
        #print(cmds)
        mc2_command = Popen(cmd, shell=True, stdout=None, stderr=None)
        mc2_command.wait() # OR  timeout based on transient queue?

        self.log.info("Finish running Motioncor2")
        self.log.info("All is good")
        msg_id = header['message-id']
        sub_id = header['subscription']
        print('MSG_ID:%s'%msg_id)
        print('Sub:%s'%sub_id)

        rw.transport.ack(header)
        rw.send([])

        self.log.info("Service complete")
        # self._shutdown()





  




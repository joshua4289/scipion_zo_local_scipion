from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe


class ScipionRunnerClearQueue(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Queue Clear Runner"

    # Logger name
    _logger_name = 'motioncor2.zocalo.services.runner'

    def initializing(self):
        '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
        queue_name = "scipion_runner"
        self.log.info("queue that is being listended to is %s" % queue_name)
        #self._transport.subscribe(queue_name,self.consume_message,acknowledgement=True)
        workflows.recipe.wrap_subscribe(self._transport,queue_name,self.consume_message, acknowledgement=True, log_extender=self.extend_log,allow_non_recipe_messages=True)


    def consume_message(self,rw,header,message):
        self.log.info('Clearing the queue')
        #self._transport.ack(message)
        #rw.send([])
        print ("queue clear")
        self.transport.ack(header)
        # workflows.recipe.wrap_subscribe(self._transport, queue_name,
        #                                 self.clear_queue_motioncor2, acknowledgement=True, log_extender=self.extend_log,
        #                                 allow_non_recipe_messages=True)

    # def clear_queue_motioncor2(self, rw, header, message):
    #     self.log.info("Starting clearing Motioncor2")
    #
    #
    #
    #     self.log.info("Finish clearing Motioncor2")
    #     self.log.info("All is good")
    #     msg_id = header['message-id']
    #     sub_id = header['subscription']
    #     print('MSG_ID:%s' % msg_id)
    #     print('Sub:%s' % sub_id)
    #
    #     rw.transport.ack(header)
    #     rw.send([])
    #
    #     self.log.info("Service complete")
        # self._shutdown()










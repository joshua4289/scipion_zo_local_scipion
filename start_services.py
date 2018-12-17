#
# dlstbx.service
#   Starts a workflow service
#

# Note that the call semantics of dlstbx.service differs from other dlstbx
# commands. dlstbx.service defaults to running in the testing ActiveMQ
# namespace (zocdev), rather than the live namespace (zocalo). This is to
# stop servers started by developers on their machines accidentally interfering
# with live data processing.
# To run a live server you must specify '--live'


from __future__ import absolute_import, division, print_function

import logging
import os
import sys

import dlstbx.util
import workflows
import workflows.contrib.start_service
import workflows.logging
from dlstbx import enable_graylog
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.version import dlstbx_version

class DLSTBXServiceStarter(workflows.contrib.start_service.ServiceStarter):
    __frontendref = None
    use_live_infrastructure = False

    def __init__(self):
        # initialize logging
        self.setup_logging()

        self.log.debug('Loading dlstbx workflows plugins')

        dlstbx = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        self.log.debug('Loading dlstbx credentials')

        # change settings when in live mode
        default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
        if '--live' in sys.argv:
            self.use_live_infrastructure = True
            default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'

        # override default stomp host
        from workflows.transport.stomp_transport import StompTransport
        try:
            StompTransport.load_configuration_file(default_configuration)
        except workflows.Error as e:
            self.log.warning(e)

    def setup_logging(self):
        '''Initialize common logging framework. Everything is logged to central
       graylog server. Depending on setting messages of DEBUG or INFO and higher
       go to console.'''
        logger = logging.getLogger()
        logger.setLevel(logging.WARN)

        # Enable logging to console
        self.console = ColorStreamHandler()
        self.console.setLevel(logging.INFO)
        logger.addHandler(self.console)

        #   logging.getLogger('stomp.py').setLevel(logging.DEBUG)
        logging.getLogger('workflows').setLevel(logging.INFO)
        #FIXME: this is hard-coding don't think it's needed
        #logging.getLogger('scipion').setLevel(logging.INFO)

        self.log = logging.getLogger('dlstbx.service')
        self.log.setLevel(logging.DEBUG)

        # Enable logging to graylog
        enable_graylog()

    def on_parser_preparation(self, parser):
        parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                          default=False, help="Show debug output")
        parser.add_option("--tag", dest="tag", metavar="TAG", default=None,
                          help="Individual tag related to this service instance")
        parser.add_option("-d", "--debug", dest="debug", action="store_true",
                          default=False, help="Set debug log level for workflows")
        parser.add_option("-r", "--restart", dest="service_restart", action="store_true",
                          default=False, help="Restart service on failure")
        parser.add_option("--test", action="store_true", dest="test",
                          help="Run in ActiveMQ testing namespace (zocdev, default)")
        parser.add_option("--live", action="store_true", dest="test",
                          help="Run in ActiveMQ live namespace (zocalo)")
        self.log.debug('Launching ' + str(sys.argv))

    def on_parsing(self, options, args):
        if options.verbose:
            self.console.setLevel(logging.DEBUG)
            logging.getLogger('dials').setLevel(logging.DEBUG)
            logging.getLogger('dlstbx').setLevel(logging.DEBUG)
            logging.getLogger('xia2').setLevel(logging.DEBUG)
        if options.debug:
            logging.getLogger('workflows').setLevel(logging.DEBUG)
        self.options = options

    def before_frontend_construction(self, kwargs):
        kwargs['verbose_service'] = True
        kwargs['environment'] = kwargs.get('environment', {})
        kwargs['environment']['live'] = self.use_live_infrastructure
        return kwargs

    def on_frontend_preparation(self, frontend):
        self.log.info('Attaching ActiveMQ logging to transport')

        def logging_call(record):
            if frontend._transport.is_connected():
                try:
                    record = record.__dict__['records']
                except:
                    record = record.__dict__
                frontend._transport.broadcast('transient.log', record)

        amq_handler = workflows.logging.CallbackHandler(logging_call)
        if not self.options.verbose:
            amq_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(amq_handler)

        if self.options.service_restart:
            frontend.restart_service = True

        extended_status = {}
        if self.options.tag:
            extended_status['tag'] = self.options.tag
        for env in ('SGE_CELL', 'JOB_ID'):
            if env in os.environ:
                extended_status['cluster_' + env] = os.environ[env]
        extended_status['dlstbx'] = dlstbx_version()

        original_status_function = frontend.get_status

        def extend_status_wrapper():
            status = original_status_function()
            status.update(extended_status)
            status['mem-uss'] = dlstbx.util.get_process_uss()
            return status

        frontend.get_status = extend_status_wrapper

import workflows.services  # To add Hack to he service, but this should be done through setup.py, Please see bellow
# FIX ME : change this import to reflect a generic runner but does not face the user
from scipion_consumer import ScipionRunner
#FIXME: temporary hack for gda2 consumer
from scipion_consumer_gda2 import ScipionRunnerGda2
from clear_scipionqueue import ScipionRunnerClearQueue #Motioncor2RunnerClearQueue

from scipion_producer import ScipionProducer
from zoc_gctf_consumer import GctfRunner
from zoc_mc2_consumer import MotionCor2Runner
from zoc_gautomatch_consumer import GautomatchRunner


from scipion_consumer_dials import ScipionRunnerDials
from zoc_relion_refine_consumer import Relion2DRunner

# Hack to include the Motioncor2Runner  n known_services
workflows.services.get_known_services()
workflows.services.get_known_services.cache['MotionCor2Runner'] = MotionCor2Runner
workflows.services.get_known_services.cache['GctfRunner'] = GctfRunner
workflows.services.get_known_services.cache['ScipionRunnerGda2'] = ScipionRunnerGda2
workflows.services.get_known_services.cache['ScipionRunner'] = ScipionRunner
workflows.services.get_known_services.cache['ScipionProducer'] =ScipionProducer
workflows.services.get_known_services.cache['ScipionRunnerClearQueue']=ScipionRunnerClearQueue
workflows.services.get_known_services.cache['Relion2DRunner']=Relion2DRunner
workflows.services.get_known_services.cache['GautomatchRunner']= GautomatchRunner




#workflows.services.get_known_services.cache['ScipionThumbnailRunner']=Motioncor2RunnerClearQueue
if __name__ == '__main__':
    DLSTBXServiceStarter().run(program_name='dlstbx.service',
                               version=dlstbx_version(),
                               transport_command_channel='command')


from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen
import json
import os, re


# Active MQ Scipion Consumer started as gda2

class ScipionRunner(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Scipion  Runner"

    # Logger name
    _logger_name = 'scipion.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        queue_name = "Scipion_runner"
        self.log.info("queue that is being listended to is %s" % queue_name)
        workflows.recipe.wrap_subscribe(self._transport, queue_name,
                                        self.run_scipion, acknowledgement=True, log_extender=self.extend_log,
                                        allow_non_recipe_messages=True)

    def run_scipion(self, rw, header, message):

        import subprocess
        from subprocess import Popen



        # get the parameters

        session = rw.recipe_step['parameters']

        # build the directories,
        project_name, gda2_workspace_dir,project_path= self.create_project_paths(session)

        #TODO:remove hack
        # get the template filename for a beamline
        #template_filename ='/dls_sw/%s/scripts/templates/workflow.json' % (session['microscope'].lower())
        template_filename = '/dls_sw/%s/scripts/templates/pablo_2d_streamer.json' % (session['microscope'].lower())

        #TODO:json filename has to be written out based on session_id and beam_line
        # got the timestamp from project


        timestamp_from_project = str(project_name).split('_')[-1]
        json_basename = 'scipion_template_{}.json'.format(timestamp_from_project)
        json_filename = os.path.join(str(gda2_workspace_dir), json_basename)


        # build the json file
        self.create_json_file_from_template(template_filename, json_filename, session,gda2_workspace_dir)

        # populate the project
        self.create_project_and_run_scipion(project_name, json_filename, gda2_workspace_dir)

        self.log.info("Finish running Scipion Zocalo")
        self.log.info("All is good")
        msg_id = header['message-id']
        sub_id = header['subscription']
        # FIXME: Useful only for Debugging remove from code
        print('MSG_ID:%s' % msg_id)
        print('Sub:%s' % sub_id)
        print('header:%s' % header)

        rw.transport.ack(header)
        rw.send([])
        #except Exception as e:
        #    self.log.error("Scipion Zocalo runner could not process the message %s with the error %s" % (str(header), e))
    def shutdown_consumer(self):
        ''' Shutdown Consumer based on the timeout mentioned in the Import step of workflow '''

        pass

    def create_project_paths(self, session):
        ''' Timestamped versions of project names '''


        import shutil, os,errno

        project_path, timestamp = self.find_visit_dir_from_session_info(session)

        project_name =  str(session['session_id']) + '_' + str(timestamp)


        #Make initial project path
        try:
            os.makedirs(project_path)

        except OSError:
            self.log.warning ('Could not create path to project  %s'%(project_path))

        #Make raw, processed and ispyb dirs as gda2
        gda2_workspace_dir = os.path.join(project_path,'processed')
        gda2_raw_dir = os.path.join(project_path,'raw')
        ispyb_dir =os.path.join(project_path,'.ispyb')

        path_list = [gda2_workspace_dir,gda2_raw_dir, ispyb_dir]

        for p in path_list:
           try:
               os.makedirs(p)
               self.log.info("Folder created %s" %(p))
           except OSError:
               if os.path.exists(p):
                   self.log.warning ('path  exists %s '%(p))




        return str(project_name), str(gda2_workspace_dir),str(project_path)



    def find_visit_dir_from_session_info(self, session):
        """ Returns a path  given a microscope and session-id  from ISPyB """


        import datetime

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        #TODO:visit path cannot be constructed from user input but has to be constructed with ispyb
        #TODO:session_id not in Camel Case
        #TODO:remove hard-code path

        #  /dls/microscope/data/year/session['session_id]

        project_year = timestamp[:4]
        project_path = "/dls/tmp/jtq89441/dls/{}/data/{}/{}/".format(str(session['microscope']).lower(), project_year,session['session_id'])




        return project_path,timestamp

    def create_json_file_from_template(self, template_filename, output_filename, session, project_path):


        config_file = json.load(open(template_filename))#print("Cannot find config file ")


        #FIX:FUTURE account for TIFF case format is a useless user input
        lowRes, highRes = calculate_ctfest_range(float(session['samplingRate']))


        for i in range(len(config_file)):

            # Get a "protocol"
            prot = config_file[i]

            if prot['object.className'] == "ProtImportMovies":
                prot['dosePerFrame'] = float(session['dosePerFrame'])
                prot['numberOfIndividualFrames'] = int(session['numberOfIndividualFrames'])
                prot['samplingRate'] = float(session['samplingRate'])
                prot['filesPath'] =  str(project_path).replace('processed','raw/GridSquare*/Data')

            if prot['object.className'] == "ProtGautomatch":
                prot['particleSize'] = float(session['particleSize'])
                prot['minDist'] = float(session['minDist'])


            if prot['object.className'] == "ProtCTFFind":
                prot['findPhaseShift'] = session['findPhaseShift']
                prot['windowSize'] = float(session['windowSize'])
                prot['lowRes'] = lowRes
                prot['highRes'] = highRes

            #tags for gctf and ctffind are different so can't put under same loop
            #windowSize is a shared tag between ctfffind and gctf
            if prot['object.className'] == "ProtGctf":
                prot['windowSize'] = float(session['windowSize'])
                prot['lowRes'] = lowRes
                prot['highRes'] = highRes

            if prot['object.className'] == 'ProtRelionExtractParticles':
                boxSize = calculateBoxSize(float(session['samplingRate']), float(session['particleSize']))
                prot['boxSize'] = boxSize

            if prot['object.className'] == 'ProtMonitorISPyB':
                prot['visit'] = str(session['session_id'])

        with open(output_filename, 'w') as f:
            json.dump(config_file, f, indent=4, sort_keys=True)


    def _create_prefix_command(self, args):

        """Prefixes command to run loading modules to setup env """

        cmd = ('source /etc/profile.d/modules.sh;'
               'module unload python/ana;'
               'module unload scipion/release-1.2.1-headless;' #release-1.2.1-headless;' # .1'-zo;'  #release-1.2-binary;'
               'module load scipion/release-1.2.1-headless;'   #release-1.2-binary;'                             # release-1.2-binary;'  # 1;'#-zo;'
               'export SCIPION_NOGUI=true;'
               'export SCIPIONBOX_ISPYB_ON=True;'
               )
        return cmd + ' '.join(args)

    def create_project_and_run_scipion(self, project_name, project_json, gda2_workspace_dir):
        """
        Starts a project in a given visit folder with a json workflow
        :type project_json: object
        """
        create_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python', 'scripts/create_project.py', project_name,
                               project_json, gda2_workspace_dir]
        create_project_cmd = self._create_prefix_command(create_project_args)

        print("the command")
        print(create_project_cmd)


        p1 = Popen(create_project_cmd, cwd=str(gda2_workspace_dir), stderr=PIPE, stdout=PIPE, shell=True)
        out_project_cmd, err_project_cmd = p1.communicate()

        print(err_project_cmd)
        if p1.returncode != 0:

            raise Exception("Could not create project ")
        else:
            schedule_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python',
                                     '$SCIPION_HOME/scripts/schedule_project.py', project_name]
            schedule_project_cmd = self._create_prefix_command(schedule_project_args)
            Popen(schedule_project_cmd, cwd=str(gda2_workspace_dir), shell=True)
            print("schedule command is " + schedule_project_cmd)


emanBoxSizes=[32, 36, 40, 48, 52, 56, 64, 66, 70, 72, 80, 84, 88,
             100, 104, 108, 112, 120, 128, 130, 132, 140, 144, 150, 160, 162, 168, 176, 180, 182, 192, 
             200, 208, 216, 220, 224, 240, 256, 264, 288, 300, 308, 320, 324, 336, 338, 352, 364, 384,
             400, 420, 432, 448, 450, 462, 480, 486, 500, 504, 512, 520, 528, 546, 560, 576, 588, 
             600, 640, 648, 650, 660, 672, 686, 700, 702, 704, 720, 726, 728, 750, 768, 770, 784, 
             800, 810, 840, 882, 896, 910, 924, 936, 972, 980, 1008, 1014, 1020, 1024]

# Calculation functions
def calculateBoxSize(samplingRate, particleSize):

    
    exactBoxSize = int((particleSize*2)/samplingRate)* 1.5
    for bs in emanBoxSizes:
        if bs >= exactBoxSize:
            return bs

    return 1024

def calculate_ctfest_range(samplingRate):

    if samplingRate < 1:
        return (0.01, 0.12)
    else:
        return (0.03, 0.12)

from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen
import json
import os, re

# this was done duting p45 testing

#import tensorflow
#tensorflow.python.client import device_lib
#resource_list = device_lib.list_loval_devices()
# if r in resource_list:
#if r['GPU'] exists


# Active MQ Scipion Consumer started as gda2

class ScipionRunnerGda2(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Scipion  Runner"

    # Logger name
    _logger_name = 'scipion.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        queue_name = "scipion_runner"
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
        template_filename = '/dls_sw/%s/scripts/templates/workflow_v1_2_from_v11_noispyb.json' % (session['microscope'].lower())

        #TODO:json filename has to be written out based on session_id and beam_line
        # got the timestamp from project


        timestamp_from_project = str(project_name).split('_')[-1]
        json_basename = 'scipion_template_{}.json'.format(timestamp_from_project)
        json_filename = os.path.join(str(gda2_workspace_dir), json_basename)


        # build the json file
        self.create_json_file_from_template(template_filename, json_filename, session,gda2_workspace_dir)
        self.log.info("JSON file written")

        
        self._start_refresh_project(project_name)
        self.log.info("gda2 project daemon started GOOD")

        # populate the project
        self.create_project_and_run_scipion(project_name, json_filename, gda2_workspace_dir)
        self.log.info("Project directories created !!")

        self.log.info("Finish running Scipion Zocalo")
        self.log.info("All is good")
        self.log.info('%s'%gda2_workspace_dir)
        self.log.info('Project name is %s' %project_name)
        msg_id = header['message-id']
        sub_id = header['subscription']
        # FIXME: Useful only for Debugging remove from code
        #print('MSG_ID:%s' % msg_id)
        #print('Sub:%s' % sub_id)
        #print('header:%s' % header)

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

        gda2_workspace_dir = os.path.join(project_path,'processed')
        gda2_raw_dir = os.path.join(project_path,'raw')


        project_name =  str(session['session_id']) + '_' + str(timestamp)

        ##FIXME:GET THE TIMESTAMP FROM HERE AND MAKE JSON FILENAME


        #os.makedirs(gda2_workspace_dir)

        #Don't make the project path it will be created by the rsync script 
         
        # try:
        #     os.makedirs(project_path)
        # except OSError:
        #     self.log.warning ('Could not create path to project  %s'%(project_path))
        #     self.log.debug('Created project path at %s'%(project_path))

        #Make raw and  processed dirs as gda2

        path_list = [gda2_workspace_dir,gda2_raw_dir]

        for p in path_list:
           try:
               os.makedirs(p)
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
        project_path = "/dls/{}/data/{}/{}/".format(str(session['microscope']).lower(), project_year,session['session_id'])




        return project_path,timestamp

    def create_json_file_from_template(self, template_filename, output_filename, session, project_path):


        config_file = json.load(open(template_filename))#print("Cannot find config file ")


        #TODO:FIX:FUTURE account for TIFF case format is a useless user input




        for i in range(len(config_file)):

            if config_file[i]['object.className'] == "ProtImportMovies":
                config_file[i]['dosePerFrame'] = float(session['dosePerFrame'])
                config_file[i]['numberOfIndividualFrames'] = int(session['numberOfIndividualFrames'])
                config_file[i]['samplingRate'] = float(session['samplingRate'])
                config_file[i]['filesPath'] =  str(project_path).replace('processed','raw/GridSquare*/Data')

            if config_file[i]['object.className'] == "ProtGautomatch":
                config_file[i]['particleSize'] = float(session['particleSize'])
                config_file[i]['minDist'] = float(session['minDist'])

            if config_file[i]['object.className'] == "ProtCTFFind":
                config_file[i]['findPhaseShift'] = session['findPhaseShift']
                config_file[i]['windowSize'] = float(session['windowSize'])

            #tags for gctf and ctffind are different so can't put under same loop
            #windowSize is a shared tag between ctfffind and gctf
            if config_file[i]['object.className'] == "ProtGctf":
                config_file[i]['windowSize'] = float(session['windowSize'])

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


    def _start_refresh_project(self,project_name):
        """ starts the script in scripts/refresh_project.py """
        refresh_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python','scripts/refresh_project.py', project_name]

        refresh_project_cmd = self._create_prefix_command(refresh_project_args)

        print(refresh_project_cmd)

        return refresh_project_cmd






    def create_project_and_run_scipion(self, project_name, project_json, gda2_workspace_dir):
        """
        Starts a project in a given visit folder with a json workflow
        :type project_json: object
        """
        import time
        create_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python', 'scripts/create_project.py', project_name,
                               project_json, gda2_workspace_dir]
        create_project_cmd = self._create_prefix_command(create_project_args)




        p1 = Popen(create_project_cmd, cwd=str(gda2_workspace_dir), stderr=PIPE, stdout=PIPE, shell=True)
        time.sleep(2)
        out_project_cmd, err_project_cmd = p1.communicate()
        self.log.info("Create project script SUCCESS")


        print(err_project_cmd)
        if p1.returncode != 0:

            raise Exception("Could not create project ")
        else:

            # TODO:testing of starting of daemon
            refresh_project_cmd = self._start_refresh_project(project_name)
            Popen(refresh_project_cmd, cwd=str(gda2_workspace_dir), shell=True)
            time.sleep(2)
            self.log.info("REFRESH PROJECT DAEMON ")


            schedule_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python',
                                     '$SCIPION_HOME/scripts/schedule_project.py', project_name]
            schedule_project_cmd = self._create_prefix_command(schedule_project_args)
            time.sleep(2)
            Popen(schedule_project_cmd, cwd=str(gda2_workspace_dir), shell=True)
            self.log.info("PROTOCOLS BEING SCHEDULED ")














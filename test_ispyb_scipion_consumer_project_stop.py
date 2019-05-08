from __future__ import absolute_import, division, print_function
from workflows.services.common_service import CommonService
import workflows.recipe
from subprocess import PIPE, Popen


from pathlib2 import Path 


# Active MQ Scipion Consumer started as gda2

class ScipionRunner(CommonService):
    '''A zocalo service for running Scipion'''

    # Human readable service name
    _service_name = "Scipion_json_finder"

    # Logger name
    _logger_name = 'scipion.zocalo.services.runner'

    def initializing(self):
        """Subscribe to the per_image_analysis queue. Received messages must be acknowledged.

		"""
        #FIXME: this is a list specific to the instance / consumer will work only in a one-beamline per consumer case
        # Add a .project file in the session which is an updated list/ json of running_projects . Only 1 project will run  all the other processed projects will be killed
        self.running_projects = list()

        queue_name = "ScipionMainDev"
        #queue_name = "TestScipionWF"
        self.log.info("queue that is being listended to is %s" % queue_name)
        workflows.recipe.wrap_subscribe(self._transport, queue_name,
                                        self.find_json_from_recipe, acknowledgement=False, log_extender=self.extend_log,
                                        allow_non_recipe_messages=True)

    






    def find_json_from_recipe(self, rw, header, message):
        self.log.info("Start running scipion json finder ")

        import subprocess
        from subprocess import Popen

        # get the parameters
        # rw is None
        # header is the timestamp

        


        #rw.send([])

        json_file = message['scipion_workflow']
        json_path = (Path(json_file))
        


        self.log.info("Initial start processing json %s " %json_file)
        self.log.info("Finish running scipion workflow finder ")




        self.find_session_id(json_path)

        project_name = self.create_project_and_process(json_path)

        # check and kill any old scipion projects in the visit

        #self._on_message(project_name)

        #rw.transport.ack(header)
        #rw.send([])





        return json_path


    def find_session_id(self,json_path):
        import re
        reg1 = re.compile(r'^[a-zA-Z]*[0-9].*')
        matches = list (filter(reg1.match,json_path.parts))
        # this is general to allow changes in format for future sessions
        session_id = matches[-1]
        
        return session_id


    @staticmethod
    def create_timestamp():
    # type: () -> str
        import datetime
        return datetime.datetime.now().strftime("%y%m%d%H%M%S")


    @staticmethod
    def sleeper(seconds):
        import time
        return time.sleep(seconds)




    def _find_gain_path(self):

        '''looks for gain file in a pre-defined location should be relative to where the the other parts '''

        pass


    def _on_message(self,project_name,visit_dir):

        ''' On new start project message stop  previous running projects '''

        self.log.info("Scipion running projects are ".format(self.running_projects))

        #TODO: On a new message write out the in-memory list to .projects/projects.txt


        projects_file = visit_dir/'.projects'/'project.txt'

        with open (str(projects_file) , 'a+') as pf:

            pf.write(project_name)
            pf.write("\n")
            self.log.info("project added to .projects file is %s " % project_name)



        #TODO: Read txt file and stop project instead of the in-memory list because method needs to be independent of the instance of the consumer

        with open(str(projects_file),'r') as pf:
            project_list = pf.readlines()
            self.log.info("list of projects is %s " % project_list)

            while len(project_list) > 1:
                to_stop = project_list.pop()

                self.log.info("%s will be stopped " % to_stop)
                stop_project_args = ['cd', '$SCIPION_HOME;', 'scipion', '--config $SCIPION_HOME/config/scipion.conf','python', 'scripts/stop_project.py', to_stop]
                stop_project_cmd = self._create_prefix_command(stop_project_args)
                try:
                    p1 = Popen(stop_project_cmd,shell=True)
                    out,err = p1.communicate()
                    if p1.returncode != 0:
                        self.log.info("%s could not be stopped " %to_stop)
                        self.log.info("%s was returned by stop project " %out )
                        self.log.info("%s error was returned by stop_project " %err)
                except:
                    pass




        # if project_name in self.running_projects and len(self.running_projects) > 1:
        #
        #     while len(self.running_projects) != 1:
        #
        #         project_to_stop = self.running_projects.pop(0)
        #
        #         stop_project_args =['cd', '$SCIPION_HOME;', 'scipion', '--config $SCIPION_HOME/config/scipion.conf','python', 'scripts/stop_project.py', project_to_stop]
        #
        #         stop_project_cmd = self._create_prefix_command(stop_project_args)
        #         self.log.info("STOP PROJECT WAS RUN FOR %s " %(project_to_stop))
        #     else:
        #         self.log.info("project left in queue is %s" %(self.running_projects))






    def _start_refresh_project(self,project_name):
        """ starts the script in scripts/refresh_project.py """
        refresh_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python','scripts/refresh_project.py', project_name]

        refresh_project_cmd = self._create_prefix_command(refresh_project_args)

        #print(refresh_project_cmd)

        return refresh_project_cmd



    def _create_prefix_command(self, args):

        """Prefixes command to run loading modules to setup env """

        cmd = ('source /etc/profile.d/modules.sh;'
               'module unload python/ana;'
               'module unload scipion/release-1.2.1-headless;'
               'module load scipion/release-1.2.1-headless;'   
               'export SCIPION_NOGUI=true;'
               'export SCIPIONBOX_ISPYB_ON=True;'
               )
        return cmd + ' '.join(args)

    def load_json(self, json_path):
       # Path --> json_dict
       import json
       json_data = json.load(open(str(json_path)))
       return json_data



        #return

        # json_to_modify = open(json_path, 'w+')
        # self.log.info('ispyb json loaded')
        # #self.log('json to modify cannot be loaded')
        # return json_to_modify

    def create_project_and_process(self,json_path):

        ''' start processing by calling various functions  '''

        timestamp = self.create_timestamp()
        visit_dir = json_path.parents[1]
        project_name = visit_dir.stem + '_' + timestamp
        workspace_dir = visit_dir / 'processed'
        workspace_dir.mkdir(parents=True, exist_ok=True)
        workflow = (workspace_dir / ("scipion_template_" + timestamp)).with_suffix(".json")

        special_project_dir = visit_dir/'.projects'
        special_project_dir.mkdir(parents=True, exist_ok=True)

        # This is important because of ACL's on the BEAMLINE has to get correct groups
        # instead of running with the json directly we need to modify some parms from what is posted to determine box size
        # instead of the copy load json into memory and make modifications







        json_to_modify = self.load_json(json_path)





        # pathlib2 object read / modify values and write out
        # TODO:
        # FIXME:find a cleaner way to do this without list indices this should be a dictionary then it's independent of the type of template json
        # NOTE : These should only be modifications pertaining to CTF fixes < 0.5 A/Px and
        # extract box size calculations rest is handled during the writing of the initial json




        lowRes, highRes = self.calculate_ctfest_range(float(json_to_modify[0]['samplingRate']))
        boxSize = self.calculateBoxSize(float(json_to_modify[0]['samplingRate']), float(json_to_modify[4]['particleSize']))

        # substitutions in  config file (inplace)

        # if json_to_modify['object.className'] == "ProtImportMovies":
        #         #TODO:Gain file is missing here
        #         pass
        #
        if json_to_modify[4]['object.className'] == "ProtGautomatch":
             json_to_modify[4]['boxSize'] = boxSize
        #
        if json_to_modify[3]['object.className'] == "ProtCTFFind":
             json_to_modify[3]['lowRes'] = lowRes
             json_to_modify[3]['highRes'] = highRes
        #
        if json_to_modify[2]['object.className'] == "ProtGctf":
             json_to_modify[2]['lowRes'] = lowRes
             json_to_modify[2]['highRes'] = highRes
        #

        import shutil
        shutil.copy(str(json_path), str(workflow))


        create_project_args = ['cd', '$SCIPION_HOME;','scipion', '--config $SCIPION_HOME/config/scipion.conf','python',
                               'scripts/create_project.py', project_name, str(workflow), str(workspace_dir)]


        create_project_cmd = self._create_prefix_command(create_project_args)

        p1 = Popen(create_project_cmd, cwd=str(workspace_dir), stderr=PIPE, stdout=PIPE, shell=True)
        self.sleeper(2)
        global out_project_cmd ,err_project_cmd
        out_project_cmd, err_project_cmd = p1.communicate()

        self.log.error(err_project_cmd)
        self.log.info("Create project script SUCCESS")
        self.log.info('processing started using parmaters in %s ' %workflow)
        self.log.info('scipion  project started %s'%visit_dir)






        if p1.returncode != 0:
            
            self.log.error("Could not create project at {}".format(visit_dir))
            self.log.error("create project error at {}".format(out_project_cmd,err_project_cmd))

            raise Exception("Could not create project ")
        else:

            self._on_message(str(project_name), visit_dir)

            #self.running_projects.append(project_name)

            #self.log.info("%s project  has been added to list of runs "%project_name)

            schedule_project_args = ['cd', '$SCIPION_HOME;', 'scipion','--config $SCIPION_HOME/config/scipion.conf', 'python',
                                     '$SCIPION_HOME/scripts/schedule_project.py', project_name]
            schedule_project_cmd = self._create_prefix_command(schedule_project_args)
            Popen(schedule_project_cmd, cwd=str(workspace_dir), shell=True)


            self.sleeper(2)
            self.log.info("schedule command is ".format(schedule_project_cmd))

            refresh_project_cmd = self._start_refresh_project(project_name)
            Popen(refresh_project_cmd, cwd=str(workspace_dir), shell=True)



            # refresh_project_cmd = self._start_refresh_project(project_name)
            # Popen(refresh_project_cmd, cwd=str(workspace_dir), shell=True)
            #
            #
            # self.sleeper(2)
            # self.log.info("gui refresh daemon")

            return project_name





    # def _find_running_project_names(self,directory):
    #     '''lists running projects in  and adds to .projects/projects.txt latest project folder on the top'''
    #     find_running_project_args = ['ls','-ltd','*/']                                                              #['find','.','-type','d']
    #
    #
    #
    #     p1  = Popen(find_running_project_args, cwd=str(directory), shell=True)
    #     out, err = p1.communicate()
    #     self.log.info(out)
    #
    #
    #
    #     with open('projects.txt','w+') as projects_file:
    #         projects_file.write(out)
    #



    def calculateBoxSize(self,samplingRate, particleSize):
        emanBoxSizes = [32, 36, 40, 48, 52, 56, 64, 66, 70, 72, 80, 84, 88,
                        100, 104, 108, 112, 120, 128, 130, 132, 140, 144, 150, 160, 162, 168, 176, 180, 182, 192,
                        200, 208, 216, 220, 224, 240, 256, 264, 288, 300, 308, 320, 324, 336, 338, 352, 364, 384,
                        400, 420, 432, 448, 450, 462, 480, 486, 500, 504, 512, 520, 528, 546, 560, 576, 588,
                        600, 640, 648, 650, 660, 672, 686, 700, 702, 704, 720, 726, 728, 750, 768, 770, 784,
                        800, 810, 840, 882, 896, 910, 924, 936, 972, 980, 1008, 1014, 1020, 1024]

        exactBoxSize = int((particleSize * 2) / samplingRate) * 1.2
        for bs in emanBoxSizes:
            if bs >= exactBoxSize:
                return bs

        return 1024


    def calculate_ctfest_range(self,samplingRate):

        if samplingRate < 1:
            return (0.01, 0.35)
        else:
            return (0.03, 0.35)


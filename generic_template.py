#!/usr/bin/env python


from __future__ import absolute_import, division, print_function

import sys
import uuid

import workflows.recipe
from workflows.transport.stomp_transport import StompTransport

def update_jobid(rw, headers, message):
    print('update job id')
    print(headers)


if __name__ == '__main__':

    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
    # override default stomp host
    try:
        StompTransport.load_configuration_file(default_configuration)
    except workflows.Error as e:
        print("Error: %s\n" % str(e))

    # StompTransport.add_command_line_options(parser)
    # (options, args) = parser.parse_args(sys.argv[1:])
    stomp = StompTransport()

    message = {'recipes': [],
               'parameters': {},
               }

    reply_to = 'transient.scipion.%s'%str(uuid.uuid4())
    print('reply to %s'%reply_to)

    # Build a custom recipe

    recipe = {}
    recipe['1'] = {}
    recipe['1']['service'] = "motioncor2_runner"
    recipe['1']['queue'] = "motioncor2_runner"
    recipe['1']['parameters'] = {}
    recipe['1']['parameters']= sys.argv[1:]
    recipe['1']['output'] = 2
    
    recipe['2'] = {}
    recipe['2']['service'] = "scipion_call_back"
    recipe['2']['queue'] = reply_to
    recipe['2']['parameters'] = {}
    recipe['2']['parameters']= "diamond"
    recipe['2']['output'] = 3
    recipe['3'] = {}

    

    print (sys.argv)


    # recipe['1']['parameters'][
    #     'arguments'] = 'module load scipion && /dls_sw/apps/scipion/release-1.1-headless-devel/scipion/software/em/motioncor2-1.0.0/bin/motioncor2 -InMrc GroEL_29-100_0000.mrc -OutMrc GroEL_29-100_0000_aligned_mic.mrc -kV 300.0 -Trunc 0 -FmDose 0.4 -OutStack 0 -Gpu 0 -InitDose 0.0 -PixSize 0.4 -Tol 0.5 -Group 1 -Patch 5 5 -Throw 0 -LogFile micrograph_000029.log -FtBin 1.0 -MaskCent 0 0 -MaskSize 1 1 -Gain /dls/ebic/data/staff-scratch/Joshua/test_full_set/Micrographs/SuperRef_GroEL_29-1_0000.mrc'

    recipe['start'] = [[1, []]]

    message['custom_recipe'] = recipe
    print("********************************* THIS IS THE SUBMITTED RECIPE*******************************************")

    stomp.connect()
    test_valid_recipe = workflows.recipe.Recipe(recipe)

    test_valid_recipe.validate()
    print(message)

    stomp.send(
        'processing_recipe',

        message
        #headers={'reply-to':'scipion_call_back'}
    )
    print("\nMotioncor2 job submitted")

    
    stomp2 = StompTransport()
    stomp2.connect()
    # stomp2._subscribe(1,reply_to, update_jobid)
    workflows.recipe.wrap_subscribe(stomp2, reply_to, update_jobid)
    print('end')





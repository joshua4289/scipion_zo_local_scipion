

# dlstbx.go
#   Process a datacollection
#

from __future__ import absolute_import, division, print_function

import json
import sys
import os
import uuid
from optparse import SUPPRESS_HELP, OptionParser

import workflows
import workflows.recipe
from workflows.transport.stomp_transport import StompTransport


def lazy_pprint(*args, **kwargs):
  from pprint import pprint
  pprint(*args, **kwargs)

def send_current_dir_to_bash():
  print (os.getcwd()) #print is important to set the bash variable
  return os.getcwd()

if __name__ == '__main__':

  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  # override default stomp host
  try:
    StompTransport.load_configuration_file(default_configuration)
  except workflows.Error as e:
    print("Error: %s\n" % str(e))

  #StompTransport.add_command_line_options(parser)
  #(options, args) = parser.parse_args(sys.argv[1:])
  
 
  stomp = StompTransport()

  message = { 'recipes': [],
              'parameters': {},
            }
  # Build a custom recipe 
  recipe = {}
  recipe['1'] = {}
  recipe['1']['service'] = "Relion_refine_runner"
  recipe['1']['queue'] = "Relion_refine_runner"
  recipe['1']['parameters'] = {}
  recipe['1']['parameters']['arguments'] = sys.argv[1:]
  recipe['1']['parameters']['cwd'] = os.getcwd()
  
  # reply_to = 'transient.relion.%s'%str(uuid.uuid4())
  # recipe['1']['output'] = 2
  # recipe['2'] = {}
  # recipe['2']['service'] = "relion_refine_call_back"
  # recipe['2']['queue'] = reply_to
  # recipe['2']['parameters'] = {}
  # recipe['2']['output'] = 3
  # recipe['3'] = {}

  recipe['start'] = [[1, []]]
 



  message['custom_recipe'] = recipe



  #send_current_dir_to_bash()
  #print(recipe['parameters']['arguments'])

  #lazy_pprint(message)

  stomp.connect()

  test_valid_recipe = workflows.recipe.Recipe(recipe)
  test_valid_recipe.validate()



  stomp.send(
    'processing_recipe',
    message
  )

  # def relion_callback(rw, headers, message):
  #     print('IN THE CALLBACK:')
  #     print(rw.recipe_step['parameters'])
  #     print(message)
  #
  #
  # #stomp2._subscribe(1,reply_to, update_jobid)
  # stomp2 = StompTransport()
  # stomp2.connect()
  # workflows.recipe.wrap_subscribe(stomp2, reply_to, relion_callback)
  #

  # def waitForProcessToComplete():
  
  #   import time
  #   time.sleep(5*60)
  #TODO:
  # While True (wait for consumer)
    # If message succeded:
      # exit gently
    # else (error somewhere)
      # exit not gently (with error meesage)

    # Sleep some time
  #print("\nMotioncor2 job submitted")  
  

  #waitForProcessToComplete()

  
  


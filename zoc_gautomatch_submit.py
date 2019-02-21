#!/usr/bin/env python
# dlstbx.go
#   Process a datacollection
#

from __future__ import absolute_import, division, print_function

import json
import sys
import os
import re
from optparse import SUPPRESS_HELP, OptionParser

import workflows
import workflows.recipe
from workflows.transport.stomp_transport import StompTransport
import uuid





def lazy_pprint(*args, **kwargs):
    from pprint import pprint
    # pprint(*args, **kwargs)


# def get_output_file(file_path):
#   print(os.path.dirname(get_output_file()))
#   return os.path.dirname(get_output_file())

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

    #output_file = get_output_file(sys.argv[-1])

    message = {'recipes': [],
               'parameters': {},
               }
    # Build a custom recipe
    recipe = {}
    recipe['1'] = {}
    recipe['1']['service'] = "Gautomatch_runner"
    recipe['1']['queue'] = "Gautomatch_runner"
    recipe['1']['parameters'] = {}
    recipe['1']['parameters']['arguments'] = sys.argv[1:]
    recipe['1']['parameters']['cwd'] = os.getcwd()

    message['custom_recipe'] = recipe


    recipe['start'] = [[1, []]]

    stomp.connect()

    test_valid_recipe = workflows.recipe.Recipe(recipe)
    test_valid_recipe.validate()

    stomp.send(
        'processing_recipe',
        message
    )



#!/usr/bin/python
"""
This file is needed to decode the command line string sent from the BPS
plugin -> PanDA -> Edge node cluster management
-> Edge node -> Container. This file is not a part
of the BPS but a part of the payload wrapper.
It decodes the hexified command line.
"""
import os
import re
import sys
import binascii


def replace_environment_vars(cmdline):
    """
    Replaces placeholders to the actual environment variables.
    :param cmdline: `str`
        Command line
    :return: cmdline: `str`
        Processed command line
    """
    envs_to_replace = re.findall(r'<ENV:(.*?)>', cmdline)
    for env_var in envs_to_replace:
        if value := os.getenv(env_var):
            cmdline = cmdline.replace('<ENV:' + env_var + '>', value)
    return cmdline


cmd_line = str(binascii.unhexlify(sys.argv[1]).decode())
data_params = sys.argv[2].split("+")
cmd_line = replace_environment_vars(cmd_line)
cmd_line = cmd_line.replace("<FILE:runQgraphFile>", data_params[0])
for key_value_pair in data_params[1:]:
    (key, value) = key_value_pair.split(":")
    cmd_line = cmd_line.replace("{" + key + "}", value)
print(cmd_line)

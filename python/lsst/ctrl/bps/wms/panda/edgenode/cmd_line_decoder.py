#!/usr/bin/python
"""
This file is needed to decode the command line string sent from the BPS plugin -> PanDA -> Edge node
cluster management -> Edge node -> Container. Once the command line hexified it can contain symbols often
stripped such as ',",` and multiline commands.
"""
import sys
import binascii
cmdline = str(binascii.unhexlify(sys.argv[1]).decode())
dataparams = sys.argv[2].split(":")
cmdline = cmdline.replace("${filename}", dataparams[0])
if len(dataparams) > 2:
    cmdline = cmdline.replace("${qgraph-id}", dataparams[1])
    cmdline = cmdline.replace("${qgraph-node-id}", dataparams[2])
print(cmdline)

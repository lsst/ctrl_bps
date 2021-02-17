#!/usr/bin/python
"""
This file is needed to decode the command line string sent from the BPS plugin -> PanDA -> Edge node cluster management
-> Edge node -> Container. Once the command line hexified it can contain symbols often stripped such as ',",`
and multiline commands.
"""
import sys
import binascii
cmdline = str(binascii.unhexlify(sys.argv[1]).decode())
cmdline = cmdline.replace("${IN/L}", sys.argv[2])
print(cmdline)

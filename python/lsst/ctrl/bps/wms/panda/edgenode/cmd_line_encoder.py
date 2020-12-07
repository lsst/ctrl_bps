#!/usr/bin/python
import sys
import binascii
cmdline = str(binascii.unhexlify(sys.argv[1]).decode())
cmdline = cmdline.replace("${IN/L}", sys.argv[2])
print(cmdline)

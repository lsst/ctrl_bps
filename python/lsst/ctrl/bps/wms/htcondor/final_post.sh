#!/bin/bash

# Args: name DAG_STATUS RETURN
# redirect stdout/stderr to file
{
   echo "final post args = $@"
   if [ $2 -eq 0 ]; then
      ret=$3;
   else
      ret=$2
   fi
   echo "final post ret = $ret"
   exit $ret
} 2>&1 > final_post_$1.out

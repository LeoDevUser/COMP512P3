#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
#export ZKSERVER=localhost:21814,localhost:21815,localhost:21816
export ZKSERVER=tr-open-04.cs.mcgill.ca:21814,tr-open-02.cs.mcgill.ca:21814,tr-open-03.cs.mcgill.ca:21814
# export ZKSERVER=open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX

java -cp $CLASSPATH:../task:.: DistProcess 

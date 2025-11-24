#!/bin/bash

ZOOBINDIR=~/apache-zookeeper-3.8.5-bin/bin
$ZOOBINDIR/zkEnv.sh
rm -rv ./*/*.class
cd task
./compiletask.sh
cd ../dist
./compilesrvr.sh
cd ../clnt
./compileclnt.sh
cd ..

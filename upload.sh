#!/bin/bash

USER=$1
CLUSTER=$2
DIRNAME=$3

# any optional arguments you wish to pass directly to rsync go in position 4

# exclude the hidden files, target, lib, project, src... except the assembled jar
rsync $4 -v -a --include='target' --include='target/scala-2.10' --include='target/scala-2.10/*.jar' \
   --include='spot-lda-c' --include='spot-lda-c/*'  --include='*.py'  --include='*.sh' --include 'top-1m.csv' \
  --exclude='*' .  $USER@$CLUSTER:~/$DIRNAME

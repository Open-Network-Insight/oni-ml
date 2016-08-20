 #!/bin/bash

 #   lda  stage
source /etc/duxbay.conf

#  copy solution files to all nodes
for d in "${NODES[@]}" 
do
    rsync -v -a --include='target' --include='target/scala-2.10' --include='target/scala-2.10/oni-ml-assembly-1.1.jar' \
       --include='oni-lda-c' --include='oni-lda-c/*'  --include 'top-1m.csv' --include='*.sh' \
      --exclude='*' .  $d:${LUSER}/ml
done


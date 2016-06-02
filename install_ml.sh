 #!/bin/bash

 #   lda  stage
source /etc/duxbay.conf

#  copy solution files to all nodes
for d in "${NODES[@]}" 
do
    # exclude the hidden files so we don't slam around VCS data...
    rsync -v -a --exclude='.*' . $d:${LUSER}/ml
	#scp -r ${LUSER}/ml $d:${LUSER}/.    
done


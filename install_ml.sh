 #!/bin/bash
 
 #   lda  stage
source /etc/oni.conf

#  copy solution files to all nodes 
# use array scp 
for d in "${NODES[@]}" 
do 
	scp -r ${LUSER}/ml $d:${LUSER}/.    
done


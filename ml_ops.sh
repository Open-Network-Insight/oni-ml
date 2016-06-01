#!/bin/bash

# read in variables (except for date) from etc/.conf file

FDATE=$1
YR=$2
MH=$3
DY=$4
DSOURCE=$5
TOL=$6

# intermediate ML results go in hive directory
DFOLDER='hive'
DUPFACTOR=1000
export DUPFACTOR

#  pre-processing scala

source /etc/duxbay.conf
export FLOW_PATH
export DNS_PATH
export HPATH
export LUSER
export LPATH
export TOL
export KRB_AUTH

hadoop fs -rm -R ${HPATH}/word_counts
hadoop fs -rm -R ${HPATH}/lda_word_counts

#CUT=$(<${DSOURCE}_qtiles)
#export CUT

# move TDM to local file system
# do we have to do a mkdir here?
mkdir -p ${LPATH}
rm -f ${LPATH}/*.{dat, beta, other, pkl} # protect the flow_scores.csv file

#kinit -kt /etc/security/keytabs/smokeuser.headless.keytab <user-id>
time spark-submit --class "main.scala.Dispatcher" --master yarn-client --executor-memory  ${SPK_EXEC_MEM}  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g"   target/scala-2.10/oni-ml_2.10-1.1.jar ${DSOURCE}_pre_lda



hadoop fs -copyToLocal  ${HPATH}/word_counts/part-* ${LPATH}/.
cd ${LPATH}
cat part-* > doc_wc.dat
rm -f part-*

#   lda  stage
source /etc/duxbay.conf
cd ..
time python lda_pre.py ${LPATH}/
rm -f ${LPATH}/doc_wc.dat
wc -l ${LPATH}/model.dat
sleep 2
#  copy input files to all nodes in a new folder for this day's LDA results
# use array scp 
for d in "${NODES[@]}" 
do 
	echo "copying $d ${LPATH} ${LUSER}"
	scp -r ${LPATH} $d:${LUSER}/ml/.    
done
sleep 2
cd ${LDAPATH}
PROCESS_COUNT=20
time mpiexec -n ${PROCESS_COUNT} -f machinefile ./lda est 2.5 20 settings.txt ${PROCESS_COUNT} ../${FDATE}/model.dat random ../${FDATE}
sleep 10

cd ${LUSER}/ml
time python lda_post.py ${LPATH}/


# post-processing stage
source /etc/duxbay.conf
export FLOW_PATH
export DNS_PATH
export HPATH
export TOL
hadoop fs -rm ${HPATH}/doc_results.csv
hadoop fs -put ${LPATH}/doc_results.csv ${HPATH}/.
hadoop fs -rm ${HPATH}/word_results.csv
hadoop fs -put ${LPATH}/word_results.csv ${HPATH}/.

#TODO: need to pass in file path as a string

hadoop fs -rm -R -f ${HPATH}/word_counts
hadoop fs -rm -R -f ${HPATH}/scored

if [ $6 != '' ]; then TOL=$6 ; fi
export TOL


#kinit -kt /etc/security/keytabs/smokeuser.headless.keytab <user-id>

time spark-submit --class "main.scala.Dispatcher" --master yarn-client --executor-memory  ${SPK_EXEC_MEM}  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g" target/scala-2.10/oni-ml_2.10-1.1.jar ${DSOURCE}_post_lda


hadoop fs -copyToLocal ${HPATH}/scored/part-* ${LPATH}/.

cd ${LPATH}

cat part-* > ${DSOURCE}_results.csv
rm -f part-*

#op ml stage         Ingest results_all_20150618.csv into suspicious connects front end
source /etc/duxbay.conf

 #scp to UI node
 scp -r ${LPATH} ${UINODE}:${RPATH}
 


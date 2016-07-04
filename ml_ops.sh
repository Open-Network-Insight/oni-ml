#!/bin/bash

# parse and validate arguments

FDATE=$1
DSOURCE=$2

YR=${FDATE:0:4}
MH=${FDATE:4:2}
DY=${FDATE:6:2}

if [[ "${#FDATE}" != "8" || -z "${DSOURCE}" ]]; then
    echo "ml_ops.sh syntax error"
    echo "Please run ml_ops.sh again with the correct syntax:"
    echo "./ml_ops.sh YYYYMMDD TYPE [TOL]"
    echo "for example:"
    echo "./ml_ops.sh 20160122 dns 1e-6"
    echo "./ml_ops.sh 20160122 flow"
    exit
fi


# read in variables (except for date) from etc/.conf file
# note: FDATE and DSOURCE *must* be defined prior sourcing this conf file

source /etc/duxbay.conf

# third argument if present will override default TOL from conf file

if [ -n "$3" ]; then TOL=$3 ; fi


# prepare parameters pipeline stages

if [ "$DSOURCE" == "flow" ]; then
    RAWDATA_PATH=${FLOW_PATH}
else
    RAWDATA_PATH=${DNS_PATH}
fi

FEEDBACK_PATH=${LPATH}/${DSOURCE}_scores.csv
DUPFACTOR=1000

PREPROCESS_STEP=${DSOURCE}_pre_lda
POSTPROCESS_STEP=${DSOURCE}_post_lda

HDFS_WORDCOUNTS=${HPATH}/word_counts

# paths for intermediate files
HDFS_DOCRESULTS=${HPATH}/doc_results.csv
LOCAL_DOCRESULTS=${LPATH}/doc_results.csv

HDFS_WORDRESULTS=${HPATH}/word_results.csv
LOCAL_WORDRESULTS=${LPATH}/word_results.csv

HDFS_SCORED_CONNECTS=${HPATH}/scores

LDA_OUTPUT_DIR=${DSOURCE}/${FDATE}

TOPIC_COUNT=20

hdfs dfs -rm -R -f ${HDFS_WORDCOUNTS}
wait

mkdir -p ${LPATH}
rm -f ${LPATH}/*.{dat,beta,gamma,other,pkl} # protect the flow_scores.csv file


time spark-submit --class "org.opennetworkinsight.Dispatcher" --master yarn-client --executor-memory  ${SPK_EXEC_MEM} \
  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false \
   --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g"   target/scala-2.10/oni-ml-assembly-1.1.jar \
   ${PREPROCESS_STEP} -i ${RAWDATA_PATH} -o ${HDFS_WORDCOUNTS} -f ${FEEDBACK_PATH} -d ${DUPFACTOR}

wait

hdfs dfs -copyToLocal  ${HDFS_WORDCOUNTS}/part-* ${LPATH}/.
wait

cd ${LPATH}
cat part-* > doc_wc.dat
rm -f part-*

#   lda  stage

cd ${LUSER}/ml
time python lda_pre.py ${LPATH}/
rm -f ${LPATH}/doc_wc.dat
wait

#  copy input files to all nodes in a new folder for this day's LDA results

for d in "${NODES[@]}" 
do 
	echo "copying $d ${LPATH} ${LUSER}"
	ssh $d 'mkdir '"'${LUSER}'"'/ml/'"'${DSOURCE}'"
	scp -r ${LPATH} $d:${LUSER}/ml/${DSOURCE}/.
done
wait

cd ${LDAPATH}

${MPI_PREP_CMD}
time ${MPI_CMD} -n ${PROCESS_COUNT} -f machinefile ./lda est 2.5 ${TOPIC_COUNT} settings.txt \
  ${PROCESS_COUNT} ../${LDA_OUTPUT_DIR}/model.dat random ../${LDA_OUTPUT_DIR}
wait


cd ${LUSER}/ml
time python lda_post.py ${LPATH}/

# post-processing stage

hdfs dfs -rm -R -f ${HDFS_DOCRESULTS}
hdfs dfs -put ${LOCAL_DOCRESULTS} ${HDFS_DOCRESULTS}

hdfs dfs -rm -R -f ${HDFS_WORDRESULTS}
hdfs dfs -put ${LOCAL_WORDRESULTS} ${HDFS_WORDRESULTS}

hdfs dfs -rm -R -f ${HDFS_WORDCOUNTS}
hdfs dfs -rm -R -f ${HDFS_SCORED_CONNECTS}

#kinit -kt /etc/security/keytabs/smokeuser.headless.keytab <user-id>
wait
time spark-submit --class "org.opennetworkinsight.Dispatcher" --master yarn-client --executor-memory  ${SPK_EXEC_MEM} \
  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false \
  --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g" \
  target/scala-2.10/oni-ml-assembly-1.1.jar ${POSTPROCESS_STEP} -i ${RAWDATA_PATH} -d ${HDFS_DOCRESULTS} \
  -w ${HDFS_WORDRESULTS} -s ${HDFS_SCORED_CONNECTS} -t ${TOL}

wait

hdfs dfs -copyToLocal ${HDFS_SCORED_CONNECTS}/part-* ${LPATH}/.
wait

cd ${LPATH}

cat part-* > ${DSOURCE}_results.csv
rm -f part-*

scp -r ${LPATH} ${UINODE}:${RPATH}

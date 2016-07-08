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

hdfs dfs -rm -R -f ${HDFS_SCORED_CONNECTS}


time spark-submit --class "org.opennetworkinsight.LDA" --master yarn-client --executor-memory  ${SPK_EXEC_MEM} \
  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false    \
  --conf shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g" target/scala-2.10/oni-ml-assembly-1.1.jar \
   ${DSOURCE} \
  -i ${RAWDATA_PATH}  \
  -d ${DUPFACTOR} \
  -f ${FEEDBACK_PATH} \
  -m ${LPATH}/model.dat \
  -o ${LPATH}/final.gamma \
  -w ${LPATH}/final.beta \
  -c ${MPI_CMD} -p ${MPI_PREP_CMD} \
  -t ${PROCESS_COUNT} \
  -u ${TOPIC_COUNT} \
  -l ${LPATH} \
  -a ${LDAPATH} \
  -r ${LUSER} \
  -s ${DSOURCE} \
  -n ${NODES} \
  -h ${HDFS_SCORED_CONNECTS} \
  -e ${TOL}

wait

# move results to hdfs.
cd ${LPATH}
hadoop fs -getmerge ${HDFS_SCORED_CONNECTS}/part-* ${DSOURCE}_results.csv && hadoop fs -moveFromLocal \
    ${DSOURCE}_results.csv  ${HDFS_SCORED_CONNECTS}/${DSOURCE}_results.csv


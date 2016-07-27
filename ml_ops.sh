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
elif [ "$DSOURCE" == "dns" ]; then
    RAWDATA_PATH=${DNS_PATH}
else
    RAWDATA_PATH=${PROXY_PATH}
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

nodes=${NODES[0]}
for n in "${NODES[@]:1}" ; do nodes+=",${n}"; done

hdfs dfs -rm -R -f ${HDFS_WORDCOUNTS}
wait

mkdir -p ${LPATH}
rm -f ${LPATH}/*.{dat,beta,gamma,other,pkl} # protect the flow_scores.csv file

hdfs dfs -rm -R -f ${HDFS_SCORED_CONNECTS}

# Add -p <command> to execute pre MPI command.
# Pre MPI command can be configured in /etc/duxbay.conf
# In this script, after the line after --mpicmd ${MPI_CMD} add:
# --mpiprep ${MPI_PREP_CMD}


time spark-submit --class "org.opennetworkinsight.SuspiciousConnects" --master yarn-client --executor-memory  ${SPK_EXEC_MEM} \
  --driver-memory 2g --num-executors ${SPK_EXEC} --executor-cores 1 --conf spark.shuffle.io.preferDirectBufs=false    \
  --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize="2g" target/scala-2.10/oni-ml-assembly-1.1.jar \
   ${DSOURCE} \
  --input ${RAWDATA_PATH}  \
  --dupfactor ${DUPFACTOR} \
  --feedback ${FEEDBACK_PATH} \
  --model ${LPATH}/model.dat \
  --topicdoc ${LPATH}/final.gamma \
  --topicword ${LPATH}/final.beta \
  --lpath ${LPATH} \
  --ldapath ${LDAPATH} \
  --luser ${LUSER} \
  --mpicmd ${MPI_CMD}  \
  --proccount ${PROCESS_COUNT} \
  --topiccount ${TOPIC_COUNT} \
  --dsource ${DSOURCE} \
  --nodes ${nodes} \
  --scored ${HDFS_SCORED_CONNECTS} \
  --threshold ${TOL}

wait

# move results to hdfs.
cd ${LPATH}
hadoop fs -getmerge ${HDFS_SCORED_CONNECTS}/part-* ${DSOURCE}_results.csv && hadoop fs -moveFromLocal \
    ${DSOURCE}_results.csv  ${HDFS_SCORED_CONNECTS}/${DSOURCE}_results.csv
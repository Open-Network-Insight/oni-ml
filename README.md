# oni-ml

Machine learning routines for OpenNetworkInsight, version 1.0.1

At present, oni-ml contains routines for performing *suspicious connections* analyses on netflow and DNS data gathered from a network. These
analyses consume a (possibly very lage) collection of network events and produces a list of the events that considered to be the least probable (or most suspicious).

oni-ml is designed to be run as a component of Open-Network-Insight. It relies on the ingest component of Open-Network-Insight to collect and load
netflow and DNS records, and oni-ml will try to load data to the operational analytics component of Open-Network-Insight.  It is strongly suggested that when experimenting with oni-ml, you do so as a part of the unified Open-Network-Insight system: Please see [the Open-Network-Insight wiki](https://github.com/Open-Network-Insight/open-network-insight/wiki)

The remaining instructions in this README file treat oni-ml in a stand-alone fashion that mayb be helpful for customizing and troubleshooting the
component.

## Getting Started



### Prerequisites, Installation and Configuration

Install and configure oni-ml as a part of the Open-Network-Insight project, per the instruction at
[the Open-Network-Insight wiki](https://github.com/Open-Network-Insight/open-network-insight/wiki).

Names and language that we will use from the configuration variables for Open-Network-Insight (that are set in the file [duxbay.conf](https://github.com/Open-Network-Insight/oni-setup/blob/1.0.1/duxbay.conf))

- MLNODE The node from which the oni-ml routines are invoked
- NODES The list of MPI worker nodes that execute the topic modelling analysis
- HUSER An HDFS user path that will be the base path for the solution; this is usually the same user that you created to run the solution
- LPATH The local path for the ML intermediate and final results, dynamically created and populated when the pipeline runs
- HPATH Location for storing intermediate results of the analysis on HDFS.


### Prepare data for input 

Load data for consumption by oni-ml by running [oni-ingest](https://github.com/Open-Network-Insight/oni-ingest/tree/1.0.1).

The data format and location where the data is stored differs for netflow and DNS analyses.

**Netflow Data**

Netflow data for the year YEAR, month  MONTH, and day DAY is stored in HDFS at `HUSER/flow/csv/y=YEAR/m=MONTH/d=DAY/*`

Data for oni-ml netflow analyses is currently stored in text csv files using the following schema zero-indexed columns:

0. time: String
1. year: Double
2. month: Double
3. day: Double
4. hour: Double
5. minute: Double
6. second: Double
7. time of duration: Double
8. source IP: String
9. destination IP: String
10. source port: Double
11. dport: Double
12. proto: String
13. flag: String
14. fwd: Double
15. stos: Double
16. ipkt: Double.
17. ibyt: Double
18. opkt: Double
19. obyt: Double
20. input: Double
21. output: Double
22. sas: String
23. das: Sring
24. dtos: String
25. dir: String
26. rip: String

**DNS Data**

DNS data for the year YEAR, month MONTH and day DAY is stored in Hive at `HUSER/dns/hive/y=YEAR/m=MONTH/d=DAY/`

The Hive tables containing DNS data for oni-ml analyses have the following schema:

0. frame_time: STRING
2. unix_tstamp: BIGINT
3. frame_len: INT
4. ip_dst: STRING
5. ip_src: STRING
6. dns_qry_name: STRING
7. dns_qry_class: STRING
8. dns_qry_type: INT
9. dns_qry_rcode: INT
10. dns_a: STRING

### Run a suspicous connects analysis

To run a suspicious connects analysis, execute the  `ml_ops.sh` script in the ml directory of the MLNODE.
```
./ml_ops.sh YYYMMDD <type> <suspicion threshold>
```

For example:  
```
./ml_ops.sh 19731231 flow 1e-20
```
and
```
./ml_ops.sh 20150101 dns 1e-4
```
### oni-ml output

A successful run of oni-ml will create and populate a directory at `LPATH/YYYYMMDD` where `YYYYMMDD` is the date argument provided to `ml_ops.sh`

This directory will contain the following files:

- flow_results.csv Network events annotated with estimated probabilities and sorted in ascending order.
- doc_results.csv  The per-document topic-mix scores. Each line represents the topic mix of a document. First entry is the document (an IP), and this is separated from the remaineder by a comma. The remainder is a space-separated list of floating point numbers that sums to 1.0. The number at position k is the fraction of the document assigned to topic k.
- word_results.csv The per-word probability by topic scores. Each line represent the conditional probabilities of a word. First entry is the word (a summarized network event), and this is separated from the remainder by a comma. The remainder of the line is a space-separated list of floating-point numbers. The number at position k is the probability of seeing this word conditioned on being in topic k.
- doc.dat An intermediate file mapping integers to IP addresses.
- words.dat An intermediate file mapping integers to the network event "words"
- model.dat An intermediate file in which each line corresponds to a "document" (an IP pair or DNS client), and contains the size of the document and the list of "words" (simplified network events) occurring in the document with their frequencies. Words are encoded as integers per the file words.dat. 
- final.beta  A space-separated text file that contains the logs of the probabilities of each word given each topic. Each line corresponds to a topic and the words are columns. 
- final.gamma A space-separated text file that contains the unnormalized probabilities of each topic given each document. Each line corresponds to a document and the topics are the columns.
- final.other  Auxilliary information from the LDA run: Number of topics, number of terms, alpha.
- likelihood.dat Convergence information for the LDA run.

In addition, on each worker node identified in NODES, in the `LPATH/YYYYMMDD` directory files of the form `<worker index>.beta` and `<workder index>.gamma`, these are local temporary files that are combined to form `final.beta` and `final.gamma`, respectively.

## Licensing

oni-ml is licensed under Apache Version 2.0

## Contributing

Create a pull request and contact the maintainers.

## Issues

Report issues at the [OpenNetworkInsight issues page](https://github.com/Open-Network-Insight/open-network-insight/issues).

## Maintainers

[Ricardo Barona](https://github.com/rabarona)

[Nathan Segerlind](https://github.com/NathanSegerlind)

[Everardo Lopez Sandoval](https://github.com/EverLoSa)

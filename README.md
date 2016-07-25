# oni-ml

Machine learning routines for OpenNetworkInsight, version 1.1

At present, oni-ml contains routines for performing *suspicious connections* analyses on netflow, DNS or proxy data gathered from a network. These
analyses consume a (possibly very lage) collection of network events and produces a list of the events that considered to be the least probable (or most suspicious).

oni-ml is designed to be run as a component of Open-Network-Insight. It relies on the ingest component of Open-Network-Insight to collect and load
netflow and DNS records, and oni-ml will try to load data to the operational analytics component of Open-Network-Insight.  It is strongly suggested that when experimenting with oni-ml, you do so as a part of the unified Open-Network-Insight system: Please see [the Open-Network-Insight wiki](https://github.com/Open-Network-Insight/open-network-insight/wiki)

The remaining instructions in this README file treat oni-ml in a stand-alone fashion that might be helpful for customizing and troubleshooting the
component.

## Getting Started



### Prerequisites, Installation and Configuration

Install and configure oni-ml as a part of the Open-Network-Insight project, per the instruction at
[the Open-Network-Insight wiki](https://github.com/Open-Network-Insight/open-network-insight/wiki).

The oni-ml routines must be built into a jar stored at `target/scala-2.10/oni-ml-assembly-1.1.jar` on the master node. This requires Scala 2.10 or later to be installed on the system building the jar. To build the jar, from the top-level of the oni-ml repo, execute the command `sbt assembly`.

Names and language that we will use from the configuration variables for Open-Network-Insight (that are set in the file [duxbay.conf](https://github.com/Open-Network-Insight/oni-setup/blob/dev/duxbay.conf))

- MLNODE The node from which the oni-ml routines are invoked
- NODES The list of MPI worker nodes that execute the topic modelling analysis
- HUSER An HDFS user path that will be the base path for the solution; this is usually the same user that you created to run the solution
- LPATH The local path for the ML intermediate and final results, dynamically created and populated when the pipeline runs
- HPATH Location for storing intermediate results of the analysis on HDFS.
- MPI_CMD : command for executing MPI on your system, eg. `mpiexec` or `mpiexec.hydra` This will vary with your MPI installation!
- MPI_PREP_CMD : a command that must be run before executing MPI on your system, such as sourcing a file of environment variables or exporting a path. May be empty. Will vary with your MPI installation.
- PROCESS_COUNT : Number of processes executing in MPI.

### Prepare data for input 

Load data for consumption by oni-ml by running [oni-ingest](https://github.com/Open-Network-Insight/oni-ingest/tree/dev).

The data format and location where the data is stored differs for netflow and DNS analyses.

**Netflow Data**

Netflow data for the year YEAR, month  MONTH, and day DAY is stored in HDFS at `HUSER/flow/csv/y=YEAR/m=MONTH/d=DAY/*`

Data for oni-ml netflow analyses is currently stored in text csv files using the following schema:

- time: String
- year: Double
- month: Double
- day: Double
- hour: Double
- minute: Double
- second: Double
- time of duration: Double
- source IP: String
- destination IP: String
- source port: Double
- dport: Double
- proto: String
- flag: String
- fwd: Double
- stos: Double
- ipkt: Double.
- ibyt: Double
- opkt: Double
- obyt: Double
- input: Double
- output: Double
- sas: String
- das: Sring
- dtos: String
- dir: String
- rip: String

**DNS Data**

DNS data for the year YEAR, month MONTH and day DAY is stored in Hive at `HUSER/dns/hive/y=YEAR/m=MONTH/d=DAY/`

The Hive tables containing DNS data for oni-ml analyses have the following schema:

- frame_time: STRING
- unix_tstamp: BIGINT
- frame_len: INT3. ip_dst: STRING
- ip_src: STRING
- dns_qry_name: STRING
- dns_qry_class: STRING
- dns_qry_type: INT
- dns_qry_rcode: INT
- dns_a: STRING

**PROXY DATA**

- proxy_date: STRING
- proxy_time: STRING  
- proxy_clientip: STRING                           
- proxy_host: STRING    
- proxy_reqmethod: STRING                                    
- proxy_useragent: STRING                                      
- proxy_resconttype: STRING                                      
- proxy_duration: INT                                         
- proxy_username: STRING                                      
- proxy_authgroup: STRING                                      
- proxy_exceptionid: STRING                                      
- proxy_filterresult: STRING                                      
- proxy_webcat: STRING                                      
- proxy_referer: STRING                                      
- proxy_respcode: STRING                                      
- proxy_action: STRING                                      
- proxy_urischeme: STRING                                      
- proxy_uriport: STRING                                      
- proxy_uripath: STRING                                      
- proxy_uriquery: STRING                                      
- proxy_uriextension: STRING                                      
- proxy_serverip: STRING                                      
- proxy_scbytes: INT                                         
- proxy_csbytes: INT                                         
- proxy_virusid: STRING                                      
- proxy_bcappname: STRING                                      
- proxy_bcappoper: STRING                                      
- proxy_fulluri: STRING



### Run a suspicious connects analysis

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
- doc_results.csv  The per-document topic-mix scores. Each line represents the topic mix of a document. First entry is the document (an IP), and this is separated from the remainder by a comma. The remainder is a space-separated list of floating point numbers that sums to 1.0. The number at position k is the fraction of the document assigned to topic k.
- word_results.csv The per-word probability by topic scores. Each line represent the conditional probabilities of a word. First entry is the word (a summarized network event), and this is separated from the remainder by a comma. The remainder of the line is a space-separated list of floating-point numbers. The number at position k is the probability of seeing this word conditioned on being in topic k.
- doc.dat An intermediate file mapping integers to IP addresses.
- words.dat An intermediate file mapping integers to the network event "words"
- model.dat An intermediate file in which each line corresponds to a "document" (the flow traffic about an IP, or the DNS queries of a client IP), and contains the size of the document and the list of "words" (simplified network events) occurring in the document with their frequencies. Words are encoded as integers per the file words.dat. 
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

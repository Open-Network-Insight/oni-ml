# oni-ml

Machine learning routines for OpenNetworkInsight, version 1.0.1

At present, oni-ml contains routines for performing *suspicious connections* analyses on netflow and DNS data gathered from a network. These
analyses consume a (possibly very lage) collection of network events and produces a list of the events that considered to be the least probable (or most suspicious).

oni-ml is designed to be run as a component of Open-Network-Insight. It relies on the ingest component of Open-Network-Insight to collect and load
netflow and DNS records, and oni-ml will try to load data to the operational analytics component of Open-Network-Insight.

For installation and use of the Open-Network-Insight system, please see [the Open-Network-Insight wiki](https://github.com/Open-Network-Insight/open-network-insight/wiki)

The remaining instructions in this README file treat oni-ml in a stand-alone fashion.

## Getting Started



### Prerequisites

Running oni-ml requires:

1. A Hadoop cluster running HDFS, Yarn, and Apache Spark 1.3 (or higher) in Yarn mode. Deploying a Hadoop cluster is out the scope of this README, but we test with [the Cloudera Distribution of Hadoop](http://www.cloudera.com/downloads/cdh/5-7-1.html)
2. A build of [oni-lda-c](https://github.com/Open-Network-Insight/oni-lda-c) in a folder called oni-lda-c immediately below the directory from which you will install oni-ml.
3. Python 2.x, installed on the machine from which oni-ml be run. See [https://www.python.org](https://www.python.org) for installation information.


###  Configuration


### Prepare data for input 


### Run a suspicous connects pipeline


## oni-lda-c output


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


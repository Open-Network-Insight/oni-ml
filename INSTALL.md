# oni-ml

Machine learning routines for OpenNetworkInsight, version 1.1

## Prerequisites, Installation and Configuration

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


## Run a suspicious connects analysis

To run a suspicious connects analysis, execute the  `ml_ops.sh` script in the ml directory of the MLNODE.
```
./ml_ops.sh YYYMMDD <type> <suspicion threshold> <max results returned>
```


For example:  
```
./ml_ops.sh 19731231 flow 1e-20 200
```

If the max results returned argument is not provided, all results with scores below the threshold will be returned, for example:
```
./ml_ops.sh 20150101 dns 1e-4
```

As the maximum probability of an event is 1, a threshold of 1 can be used to select a fixed number of most suspicious items regardless of their exact scores:
```
./ml_ops.sh 20150101 proxy 1 2000
```



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

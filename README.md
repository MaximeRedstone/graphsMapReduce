# Finding connected components in graphs on Spark

This project aims at developing a scalable algorithm able to find connected components in graphs using Apache Spark. A report explains in more details the full methodology pursued in this project. It is located at ```reports/connected_components_graphs_report.pdf```

## Architecture

This repository contains the following files:
* ```reports```: contains the original paper used as reference for this project and the report presenting our methodology
* ```notebooks```
  * ```notebooks/databricks```: contains both Scala and PySpark implementations of the algorithm to be run on Databricks
  * ```notebooks/local```: contains a version of the algorithm that can be run on a local machine 
  * ```notebooks/analysis```: contains the data vizualization notebooks that was used in the pdf report mentioned above
* ```assets```: contains the ```png``` files of data vizualization used in the report
* ```src```: contains a python script used to parse the cluster logs produced by DataBricks. More details in the report, section 4.2
* ```generator```: contains the source code of a graph generator developed in C
* ```logs```: contains the source logs produced when executing the algorithm and used for performance analysis

## Contributors

Project realized by @hehlinge42, @louistransfer and @MaximeRedstone 

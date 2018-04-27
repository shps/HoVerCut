# HoVerCut
A Horizontally and Vertically scalable streaming graph Vertex-Cut partitioner for boosting partitioning algorithms. It enables to define different partitioning algorithms and executes them in a parallel scalable fashion. You can deploy HoVerCut either on a single machine or across multiple machines in a distributed fashion.


#### Input Parameters
input edge file: -f <file>

partitiong selection policy (partitioning algorithm): -a <e.g., hdrf or greedy>

number of partitions: -p <number>

window size: -w <size>

number of subpartitioners (threads): -t <number>

Example: -f ./graph.txt -a greedy -p 16 -w 1000 -t 16

#### Optional Parameters
type of the shared state: -storage <e.g., memory or remote>

address of remote shared state: -db <ip:port>

output file: -output <file>

shuffle the input edges: -shuffle <true/false>

to compute the exact degree before processing: -ed <true/false>

delimiter of input file: -d <e.g., ",">

append the results to output file: -append <true/false>


#### Partition Selection Policy
In HoVerCut, you can implement different partitioning algorithms as a partition selection policy. Currently, there are two partition selection policies are implemented: HDRF and PowerGraph Greedy.

In order to implement a new partition selection policy, you need to implement the PartitionSelectionPolicy interface.

#### Citing
If you find HoVerCut useful, you may cite the paper as follows:

        @inproceedings{sajjad2016boosting,
          title={Boosting vertex-cut partitioning for streaming graphs},
          author={Sajjad, Hooman Peiro and Payberah, Amir H and Rahimian, Fatemeh and Vlassov, Vladimir and Haridi, Seif},
          booktitle={Big Data (BigData Congress), 2016 IEEE International Congress on},
          pages={1--8},
          year={2016},
          organization={IEEE}
        }

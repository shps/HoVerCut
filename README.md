# HoVerCut
A Horizontally and Vertically scalable streaming graph Vertex-Cut partitioner

HoVerCut enables to define different partitioning heuristics and executes them in a parallel scalable fashion.


#### Input parameters
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

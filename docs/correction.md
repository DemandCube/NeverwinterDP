#performanceResults/perfJan2016.md

For *Kafka to Kafka Conclusions* I think we better change to Kafka to Kafka Observations, we should not conclude any thing until we have a strong proof.

A table structure would be easier to read

Static configuration such kafka, hadoop and hardware configuration and specification such cpu, memory, IO, network... 

|Hardware/Software                 | DP=8, DR=3| DP=10, DR=3 |
| -------------------------------- | --------------------------------- |------------------------------------|
| 3 m4.large HD x 5 m4.large kafka | Bytes/sec and record/sec          | Bytes/sec and record/sec           |
| 4 m4.large HD x 5 m4.large kafka | Bytes/sec and record/sec          | Bytes/sec and record/sec           |

Legend
HD: Hadoop Worker
DP : Default parallelism
DR : Default Replication

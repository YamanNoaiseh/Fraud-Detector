# Fraud Detector
#### Dude, where is my card?

## Table of Content

- [Idea](#idea)
- [Engineering Challenge](#engineering-challenge)
- [Solution](#solution)
- [Pipeline](#pipeline)
- [Performance Optimization](#performance-optimization)
- [Author](#author)

### Idea
Based on the simple idea that we keep our cellphones with us all the time, we can match the geographical locations of incoming credit card transactions with the user geographical locations, requested from thier cellphones, to identify suspicious transactions.

I am assuming that the system will get the user location data no later than 10 minutes from the time it is requested. The distance is matched based on the average speed of 8 kilometers per 10 minutes. However, a complex model could replace my simplified model *./stream/distancer.py* by replacing the *distance_threshold* function with the new logic.

### Engineering Challenge
Transactions need to be stored to be matched with incoming location pins. However, writing into an on-disk database is very expensive and caused a bottleneck. We needed a fast, distributed or sharable datastore.

### Solution
I needed a fast in-memory database that is consistent and fault tolerant.
Redis is a fast and consistent in-memory datastore. However, Redis consistency is only guaranteed when it runs on one instace (standalone Redis server).
To overcome this problem, I designed a Redis architecture that consists of two independent, standalone Redis servers, and disabled both RDB and AOF persistant/recovery log writes. Disabling the default Redis persistance options improved the speed of Redis operations. The resulting Redis architecture illustrated below served as a distributed, fault-tolerant, and consistent datastore that my system communicated with in order to find potentially fraudulent transactions.
<p align="center">
  <img src="https://user-images.githubusercontent.com/10068563/40880719-00502f1a-6684-11e8-8fe9-c8542769dd43.png" width="700" height="200"/>
</p>

### Pipeline
The final data pipeline looks like the following:
<p align="center">
  <img src="https://user-images.githubusercontent.com/10068563/40402638-d431b686-5e1a-11e8-9c22-efdec79be42f.png"/>
</p>

### Performance Optimization
The current pipeline was tested against 10000 transactions/second and was able to peocess them in real time.  
  - **Redis Pipelines:** Redis offers a bulk execution option called *Redis Pipeline*. From [Redis FAQ page](https://redis.io/topics/faq): *"using pipelining Redis running on an average Linux system can deliver even 1 million requests per second."*  
  - **Kafka Offsets:** We can also improve the performance by minimizing the network communication with Redis by keeping track of the Kafka message offset of the last successfully-inserted message while inserting only into Redis Master and having a Redis Slave ready. If the Redis Master crashes the Slave becomes Master and the system writes all messages starting from the last Kafka offset to the new Master using one Redis pipeline execution.
  - Using the two methods above together would result in a production-level performance.
  
  
### Author
Yaman Noaiseh  
yanoaiseh@gmail.com

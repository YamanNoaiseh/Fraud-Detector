# Fraud Detector
#### Dude, where is my card?


## Insight-Proejct, Boston, May 2018

Based on the simple idea that we keep our cellphones with us, we can match the geographical location of a credit card transaction with the user geographical location, requested from his/her cellphone, to identify suspicious transactions.

## Engineering Challenge
Transaction records need to be stored in order to be matched with incoming location pins. However, writing into an on-disk database is very expensive and caused a bottleneck.

## Solution
I needed a fast in-memory database that is consistent and fault tolerant.
Redis is a fast and consistent in-memory datastore. However Redis consistency is only guaranteed when it runs on one instace (standalone Redis server).
To overcome this problem, I designed a Redis architecture that consists of two independent, standalone Redis servers, and disabled both RDB and AOF persistant/recovery log writes. As a result, I gained an extra speed in a distributed, fault-tolerant, and consistent Redis cluster.

## Pipeline
The data pipeline looks like the following:

![pipeline](https://user-images.githubusercontent.com/10068563/40402541-4e14ef1e-5e1a-11e8-8268-ab329752485b.png)

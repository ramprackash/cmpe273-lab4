cmpe273-lab4
============
For Part-1: 

   The DistributedCacheService class has been changed to support CRDT. 
   When this class is instantiated, user now needs to specify how many nodes 
   are there in the service. After this, any "put" operation by client will
   put the data to all nodes and "get" will verify if any repair operation is 
   needed.

   Support for DELETE can be found in 
      1. server/src/main/java/edu/sjsu/cmpe/cache/repository/InMemoryCache.java
      2. server/src/main/java/edu/sjsu/cmpe/cache/repository/CacheInterface.java
      3. server/src/main/java/edu/sjsu/cmpe/cache/api/resources/CacheResource.java
   Rollback on WRITE implementation is found in 
      1. client/src/main/java/edu/sjsu/cmpe/cache/client/DistributedCacheService.java


   

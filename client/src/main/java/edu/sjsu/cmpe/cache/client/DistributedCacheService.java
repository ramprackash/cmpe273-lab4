package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Headers;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheServerUrl;
    private final Integer startport;
    private final Integer numservice;
    private final int writeQuorum;
    private final int readQuorum;
    private AtomicInteger numSuccess;
    private AtomicInteger numAttempts;
    private String[] getValues;
    private AtomicInteger getIndex;
    private CountDownLatch responseWaiter;



    public DistributedCacheService(String serverUrl, Integer startport, Integer numservice) {
        this.writeQuorum = this.readQuorum = 2;
        this.numSuccess = new AtomicInteger(0);
        this.numAttempts = new AtomicInteger(0);
        this.numservice = numservice;
        this.cacheServerUrl = serverUrl;
        this.startport = startport;
        this.getValues = new String[this.numservice];
        this.getIndex = new AtomicInteger(0);
        this.responseWaiter = new CountDownLatch(this.numservice);
    }

    // naive method to return string with maximum occurrence
    private String getReadQuorumString() {

        HashMap<String, Integer> cntMap = new HashMap<String, Integer>();
        int maxCount = 1;
        String  retVal = this.getValues[0];
        for (int j = 0; j < this.getIndex.get(); j++) {
            if (cntMap.containsKey(this.getValues[j])) {
                int count = cntMap.get(this.getValues[j]).intValue();
                cntMap.put(this.getValues[j], new Integer(count + 1));
                if (count + 1 > maxCount) {
                    retVal = this.getValues[j];
                    maxCount = count + 1;
                }
            } else {
                cntMap.put(this.getValues[j], 1);
            }
        }
        if (maxCount == this.numservice) {
            //Every node has the same data. No need to repair
            retVal = null;
        } else if ((maxCount < this.readQuorum) && (retVal != null)) {
            System.out.println("ReadQuorum not met. No use repairing. Give up.");
            // Does not meet the readQuorum. Do no try to repair
            for (int j = 0; j < this.getIndex.get(); j++) {
                // reset all the get values so the client sees failure
                this.getValues[j] = null;
            }
        }
        return retVal;
    }
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        String value = null;
        int i = 0;
        while (i < this.numservice.intValue()) {
            Integer port = this.startport + new Integer(i);

            String cacheUrl = this.cacheServerUrl + ":" + port.toString();
            System.out.println("Getting from server " + cacheUrl);
            Future<HttpResponse<JsonNode>> future = Unirest.get(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.numAttempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Failed to get from the cache. Might need repair");
                                } else {
                                    //System.out.println("Success ");
                                    DistributedCacheService.this.numSuccess.getAndIncrement();
                                    DistributedCacheService.this.getValues[DistributedCacheService.this.getIndex.get()] =
                                            httpResponse.getBody().getObject().getString("value");
                                    //System.out.println(DistributedCacheService.this.getIndex.get() + " => " + httpResponse.getBody().getObject().getString("value"));
                                    DistributedCacheService.this.getIndex.getAndIncrement();
                                }
                                //System.out.println("Counting down");
                                responseWaiter.countDown();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.numAttempts.getAndIncrement();
                            System.out.println("GET Failed : Might need repair");
                            responseWaiter.countDown();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.numAttempts.getAndIncrement();
                            System.out.println("Cancelled");
                            responseWaiter.countDown();
                        }
                    });

            i++;
        }
        try {
            this.responseWaiter.await();
        } catch (Exception e) {
            System.out.println("Error" + e);
        }

        String repairString = getReadQuorumString();
        if (repairString != null) {
            System.out.println("String to repair with: " + repairString);
            put(key, repairString);
        } else {
            repairString = this.getValues[0];
        }
        return repairString;
    }

    @Override
    public void delete(final long key) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.numservice.intValue(); i++) {
            Integer port = this.startport + new Integer(i);

            String cacheUrl = this.cacheServerUrl + ":" + port.toString();
            System.out.println("Deleting from " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .delete(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            if (httpResponse.getCode() != 204) {
                                System.out.println("Failed to del from the cache.");
                            } else {
                                //System.out.println("Deleted " + key);
                                DistributedCacheService.this.numSuccess.getAndIncrement();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            System.out.println("Failed : " + e);
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("Cancelled");
                        }
                    });

        }
        DistributedCacheService.this.numSuccess.set(0);
        DistributedCacheService.this.numAttempts.set(0);
    }
    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(final long key, final String value) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.numservice.intValue(); i++) {
            Integer port = this.startport + new Integer(i);

            String cacheUrl = this.cacheServerUrl + ":" + port.toString();
            System.out.println("Putting to " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .put(cacheUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJsonAsync(new Callback<JsonNode>() {
                        private void checkForSuccess() {
                            int flag = 0;
                            //System.out.println(DistributedCacheService.this.numAttempts.get() + " of " + DistributedCacheService.this.numservice);
                            if (DistributedCacheService.this.numAttempts.get() == DistributedCacheService.this.numservice) {
                                if (DistributedCacheService.this.numSuccess.get() >= DistributedCacheService.this.writeQuorum) {
                                    //System.out.println("PUT COMPLETED SUCCESSFULLY");
                                } else {
                                    flag = 1;
                                    //System.out.println("PUT FAILED - TRYING TO ROLLBACK " + DistributedCacheService.this.numSuccess.get() + " " + DistributedCacheService.this.writeQuorum);
                                }

                                DistributedCacheService.this.numAttempts.set(0);
                                DistributedCacheService.this.numSuccess.set(0);
                                if (flag == 1) {
                                    DistributedCacheService.this.delete(key);
                                }
                            }
                        }

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.numAttempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Failed to add to the cache.");
                                } else {
                                    DistributedCacheService.this.numSuccess.getAndIncrement();
                                }
                                checkForSuccess();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.numAttempts.getAndIncrement();
                            System.out.println("Failed to PUT data.");
                            checkForSuccess();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.numAttempts.getAndIncrement();
                            System.out.println("Cancelled");
                            checkForSuccess();
                        }
                    });

        }
    }
}

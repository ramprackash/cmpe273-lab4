package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheServerUrl;
    private final String getUrl;
    private final Integer startport;
    private final Integer numservice;
    private final int writeQuorum;
    private AtomicInteger numSuccess;
    private AtomicInteger numWriteAttempts;

    public DistributedCacheService(String serverUrl, Integer startport, Integer numservice) {
        this.writeQuorum = 2;
        this.numSuccess.set(0);
        this.numWriteAttempts.set(0);
        this.numservice = numservice;
        this.cacheServerUrl = serverUrl;
        this.startport = startport;
        this.getUrl = this.cacheServerUrl + ":" + this.startport.toString();

    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.get(this.getUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        String value = response.getBody().getObject().getString("value");

        return value;
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
                                System.out.println("Deleted " + key);
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
        DistributedCacheService.this.numWriteAttempts.set(0);
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
                            System.out.println(DistributedCacheService.this.numWriteAttempts.get() + " of " + DistributedCacheService.this.numservice);
                            if (DistributedCacheService.this.numWriteAttempts.get() == DistributedCacheService.this.numservice) {
                                if (DistributedCacheService.this.numSuccess.get() >= DistributedCacheService.this.writeQuorum) {
                                    System.out.println("PUT COMPLETED SUCCESSFULLY");
                                } else {
                                    flag = 1;
                                    System.out.println("PUT FAILED - TRYING TO ROLLBACK");
                                }

                                DistributedCacheService.this.numWriteAttempts.set(0);
                                DistributedCacheService.this.numSuccess.set(0);
                                if (flag == 1) {
                                    DistributedCacheService.this.delete(key);
                                }
                            }
                        }

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            DistributedCacheService.this.numWriteAttempts.getAndIncrement();
                            if (httpResponse.getCode() != 200) {
                                System.out.println("Failed to add to the cache.");
                            }
                            else {
                                System.out.println("Added " + key + " => " + value);
                                DistributedCacheService.this.numSuccess.getAndIncrement();
                            }
                            checkForSuccess();
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.numWriteAttempts.getAndIncrement();
                            System.out.println("Failed : " + e);
                            checkForSuccess();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.numWriteAttempts.getAndIncrement();
                            System.out.println("Cancelled");
                            checkForSuccess();
                        }
                    });

        }
    }
}

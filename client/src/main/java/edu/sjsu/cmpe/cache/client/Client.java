package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Unirest;

public class Client {

    public static void main(String[] args) throws Exception {
        CacheServiceInterface cache = new DistributedCacheService( "http://localhost",
                new Integer(3000),  // Start port
                new Integer(3));    // Number of servers to run

        System.out.println("STEP 1: PUT 1 => 'a' on all servers");
        cache.put(1, "a");
        System.out.println("STEP 1: Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("STEP 2: PUT 1 => 'b' on all servers");
        cache.put(1, "b");
        System.out.println("STEP 2: Sleeping for 30 seconds");
        Thread.sleep(30000);

        System.out.println("STEP 3: GET 1 from all servers");
        String value = cache.get(1);
        System.out.println("STEP 3: get(2) => " + value);
        System.out.println("Exiting Cache Client...");


        Unirest.shutdown();
    }

}

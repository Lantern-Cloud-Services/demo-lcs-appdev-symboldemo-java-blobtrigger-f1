package com.function;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.google.gson.Gson;   
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import com.azure.messaging.servicebus.*;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class BlobTriggerJava1 
{
    class SymbolHelper
    {
        public String symbol;
        public String value;
    }

    class SymbolDeltaHelper
    {
        public String symbol;
        public String delta;
        public String curValue;
        public String origOrder;
        public String procTimeStamp;
    }


    /**
     * This function will be invoked when a new or updated blob is detected at the specified path. The blob contents are provided as input to this function.
     * 
     */
    
    // by adding source to the trigger we can also make this an event grid trigger that doesn't contain the full payload
    @FunctionName("BlobTriggerJava1")
    @StorageAccount("demolcsappdevsymbolsa1_STORAGE")
    public void run(
        @BlobTrigger(name = "content", path = "symboleventsin/{name}", dataType = "binary") byte[] content,
        @BindingName("name") String name,
        final ExecutionContext context) 
    {
        context.getLogger().info("Java Blob trigger function processed a blob. Name: " + name + "\n  Size: " + content.length + " Bytes\n");

        String contentStr = new String(content);

        SymbolHelper sHelper = new SymbolHelper();
        sHelper = new Gson().fromJson(contentStr, SymbolHelper.class);
        context.getLogger().info("Symbol: " + sHelper.symbol + " Value: " + sHelper.value);
        
        SymbolDeltaHelper deltaHelper = new SymbolDeltaHelper();
        deltaHelper.symbol        = sHelper.symbol;
        deltaHelper.curValue      = sHelper.value;
        deltaHelper.origOrder     = name;
        deltaHelper.procTimeStamp = String.valueOf(System.currentTimeMillis());

        // persist to redis
        _persistToRedis(context, deltaHelper);

        // put payload onto service bus to be cached in cosmos
        String connectionString = System.getenv("SB_CON_STR");
        String queueName        = System.getenv("SB_QNAME");        

        String sbPayload = new Gson().toJson(deltaHelper);
        context.getLogger().info("SB Payload:\n" + sbPayload);
        _sendMessageToBus(connectionString, queueName, sbPayload);

        // delete blob
        String bPayload   = "{ \"blobname\": \"" + name + "\"}";
        String urlString = System.getenv("DELETEBLOB_URL"); 
        _sendHTTPPostReq(bPayload, urlString);
    }

    private void _persistToRedis(ExecutionContext aContext, SymbolDeltaHelper aSHelper)
    {
        boolean useSsl = true;
        String cacheHostname = System.getenv("REDISCACHEHOSTNAME");
        String cachekey      = System.getenv("REDISCACHEKEY");

        // Connect to the Azure Cache for Redis over the TLS/SSL port using the key.
        Jedis jedis = new Jedis(cacheHostname, 6380, DefaultJedisClientConfig.builder()
            .password(cachekey)
            .ssl(useSsl)
            .build()
        );
        
        aContext.getLogger().info("Redis ping: " + jedis.ping());

        if (!"".equals(aSHelper.curValue) && "0".equals(aSHelper.curValue))
        {
            jedis.flushDB();
            jedis.close();

            aSHelper.delta = "0";

            return;
        }

        String cacheStr = jedis.get(aSHelper.symbol);
        if (cacheStr != null && !"".equals(cacheStr))
        {
            aContext.getLogger().info("Cached hit: " + aSHelper.symbol + " -> " + cacheStr);

            Integer cacheVal = (Integer) Integer.parseInt(cacheStr);
            Integer newVal   = (Integer) Integer.parseInt(aSHelper.curValue);
            
            Integer deltaVal = newVal - cacheVal;
            aSHelper.delta = String.valueOf(deltaVal);
        }
        
        jedis.set(aSHelper.symbol, aSHelper.curValue);
        jedis.close();

        aContext.getLogger().info("Cached to Redis: " + aSHelper.symbol + " -> " + aSHelper.curValue);
    }

    private void _sendHTTPPostReq(String aPayload, String aUrlString)
    {
        try 
        {
            URL url = new URL(aUrlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    
            //Request headers
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Cache-Control", "no-cache");
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Ocp-Apim-Subscription-Key", System.getenv("APIM_API_KEY"));            
    
            // Request body
            connection.setDoOutput(true);
            connection.getOutputStream().write(aPayload.getBytes());
        
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) 
            {
                content.append(inputLine);
            }
            
            in.close();
            System.out.println("server response: " + content);
    
            connection.disconnect();
        } 
        catch (Exception ex) 
        {
            System.out.print("exception:" + ex.getMessage());
        }
    }

    private void _sendMessageToBus(String aConnectionString, String aQueueName, String aPayload)
    {
        // create a Service Bus Sender client for the queue 
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
            .connectionString(aConnectionString)
            .sender()
            .queueName(aQueueName)
            .buildClient();

        // send one message to the queue
        senderClient.sendMessage(new ServiceBusMessage(aPayload));
        
        System.out.println("Sent a single message to the queue: " + aQueueName);
    }

}
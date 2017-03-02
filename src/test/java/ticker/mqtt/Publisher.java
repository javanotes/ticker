/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package ticker.mqtt;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class Publisher implements Closeable
{
	public static final String BROKER_URL = "tcp://localhost:1883";
	public static final String TOPIC_TEMPERATURE = "home/LWT";
	
	public static final int MAX_INFLIGHT = 100;
	public static final int QoS = 0;
	public static final int NO_OF_ITEMS = 1000;
    
    private Random rand = new Random();

    private List<MqttClient> clientList = Collections.synchronizedList(new LinkedList<>());
    ThreadLocal<MqttClient> clients = ThreadLocal.withInitial(new Supplier<MqttClient>() {
    	
		@Override
		public MqttClient get() {
			String clientId = UUID.randomUUID().toString() + "-pub";
	         try
	         {
	        	 MqttClient client = new MqttClient(BROKER_URL, clientId);
	              
	              MqttConnectOptions options = new MqttConnectOptions();
	              options.setCleanSession(false);
	              options.setWill(client.getTopic(TOPIC_TEMPERATURE),
	              "I'm gone".getBytes(), 2, true);
	               
	              options.setMaxInflight(MAX_INFLIGHT);
	              System.out.println("connecting.."+clientId);
	              client.connect(options);
	              System.out.println("connected ");
	              clientList.add(client);
	              	              
	            return client;
	         }
	         catch (MqttException e)
	        {
	             e.printStackTrace();
	         }
			return null;
		}
	});
    
    
    void publish() throws MqttException
    {
    	publishTemperature();
    }
    private void publishTemperature() throws MqttException {
        final MqttTopic temperatureTopic = clients.get().getTopic(TOPIC_TEMPERATURE);

        final int temperatureNumber = rand.nextInt(50);
        final String temperature = temperatureNumber + "°C";
        //System.out.println("temperature => "+temperature);
        MqttMessage msg = new MqttMessage(temperature.getBytes(StandardCharsets.UTF_8));
        msg.setQos(QoS);
        temperatureTopic.publish(msg);
    }
    
    public Publisher()
    {

    }
    String clientId;


	@Override
	public void close() {
		for(MqttClient c : clientList)
		{
			try {
				c.disconnect();
				c.close();
			} catch (MqttException e) {
				//throw new IOException(e);
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Publisher p = new Publisher();
		ExecutorService e = Executors.newFixedThreadPool(4);
		long start = System.currentTimeMillis();
		for(int i=0; i<NO_OF_ITEMS; i++){
			e.submit(new Runnable() {
				
				@Override
				public void run() {
					try {
						p.publish();
					} catch (MqttException e) {
						e.printStackTrace();
					}
				}
			});
			
		}
		e.shutdown();
		e.awaitTermination(1, TimeUnit.HOURS);
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		System.out.println("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
		
		p.close();
	}
}

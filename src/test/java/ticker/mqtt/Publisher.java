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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class Publisher implements Closeable
{
	public static final String BROKER_URL = "tcp://localhost:1883";
	public static final String TOPIC_TEMPERATURE = "home/LWT";
	
    private MqttClient client;
    private Random rand = new Random();

    
    String open() throws MqttException
    {
    	 String clientId = UUID.randomUUID().toString() + "-pub";
         try
         {
              client = new MqttClient(BROKER_URL, clientId);
              
              MqttConnectOptions options = new MqttConnectOptions();
              options.setCleanSession(false);
              options.setWill(client.getTopic(TOPIC_TEMPERATURE),
              "I'm gone".getBytes(), 2, true);
               
              options.setMaxInflight(100);
              client.connect(options);
         }
         catch (MqttException e)
        {
             throw e;
         }
		return clientId;
    }
    void publish() throws MqttException
    {
    	publishTemperature();
    }
    private void publishTemperature() throws MqttException {
        final MqttTopic temperatureTopic = client.getTopic(TOPIC_TEMPERATURE);

        final int temperatureNumber = rand.nextInt(50);
        final String temperature = temperatureNumber + "°C";
        //System.out.println("temperature => "+temperature);
        MqttMessage msg = new MqttMessage(temperature.getBytes(StandardCharsets.UTF_8));
        msg.setQos(0);
        temperatureTopic.publish(msg);
    }
    
    public Publisher()
    {

    }
    String clientId;


	@Override
	public void close() throws IOException {
		try {
			client.disconnect();
			client.close();
		} catch (MqttException e) {
			throw new IOException(e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Publisher p = new Publisher();
		p.open();
		
		long start = System.currentTimeMillis();
		for(int i=0; i<MqttListenerTest.NO_OF_ITEMS; i++)
			p.publish();
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		System.out.println("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
		
		p.close();
	}
}

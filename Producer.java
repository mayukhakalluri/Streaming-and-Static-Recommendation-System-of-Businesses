import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.ProducerConfig;

public class Producer {
         
				public static void main(String[] argv)throws Exception {
              String topicName = "project";
              String fileName = "C:\\Users\\Students07\\Desktop\\Project BigData\\yelp_dataset_challenge_academic_dataset\\TipsNew.dat\\part-00000";
              System.out.println("Enter message(type exit to quit)");

              //Configure the Producer
              Properties configProperties = new Properties();
              configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
              configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
              configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

              org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);
              String line =null;
              
                  // FileReader reads text files in the default encoding.
                  FileReader fileReader = 
                      new FileReader(fileName);

                  // Always wrap FileReader in BufferedReader.
                  BufferedReader bufferedReader = 
                      new BufferedReader(fileReader);
                  int count=0;
                  while((line = bufferedReader.readLine()) != null) {
                	  count++;
                	  if (count==1000)
                	  {
                		  count=0;
                		  Thread.sleep(5000);
                	  }
                	  
                	  System.out.println(line);
                	  ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
                      producer.send(rec);
                	   }   

                  // Always close files.
                  bufferedReader.close();         
           
              producer.close();
          }
        }
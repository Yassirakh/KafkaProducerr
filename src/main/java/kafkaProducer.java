import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Order{
        public String nom;
        public String ordre;
        public int nmbrAction;
        public double priceAction;

        public Order(String nom, String ordre, int nmbrAction, double priceAction){
            this.nom = nom;
            this.ordre = ordre;
            this.nmbrAction = nmbrAction;
            this.priceAction = priceAction;
        }

    @Override
    public String toString() {
        return "Ordre{" +
                "'company': '" + nom + '\'' +
                ", 'typeOrdre': '" + ordre + '\'' +
                ", 'nbrAction': " + nmbrAction +
                ", 'prices': " + priceAction +
                '}';
    }
}

public class kafkaProducer {
    String message;
    Random random = new Random();
    public static Map<Integer, String> companies = new HashMap<>();
    public static Map<Integer, String> orderType = new HashMap<>();
    public static void main(String[] args) { new kafkaProducer().start();
    }

    public void start(){

        companies.put(1, "ATS");
        companies.put(2, "Facebook");
        companies.put(3, "HP");
        companies.put(4, "Lenovo");
        companies.put(5, "DataProtect");
        companies.put(6, "Void");

        orderType.put(1, "Achat");
        orderType.put(2, "Vente");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "examTopic");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
            Order order = new Order(companies.get(random.nextInt(6 - 1 + 1) + 1),orderType.get(random.nextInt(2 - 1 + 1) + 1),random.nextInt(10)+1,1000*(1 + (0 - 1) * random.nextDouble()));
            message = order.toString();
            Gson gson = new Gson();
            kafkaProducer.send(new ProducerRecord<Integer, String>("bdccTopic",null, gson.toJson(order)),(md, ex)->{
                System.out.println("Sending message "+message+" To topic "+md.topic()+" Partition "+md.partition());
            });

        },10,10, TimeUnit.MILLISECONDS);

    }
}



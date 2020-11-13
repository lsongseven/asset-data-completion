package com.envisioniot.enos.asset.data.completion;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @Author liang.song lsongseven@gmail.com
 * @Date 2020/11/12 20:03
 */
public class AssetDataCompletion {
    public static void main(String[] args) throws IOException {

        String bootstrapServer, topic, fileDest;
        Map<String, List<String>> params = new HashMap<>();
        for (int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("--")) {
                String key = args[i++];
                params.putIfAbsent(key, new ArrayList<>());
                while (i < args.length && !args[i].startsWith("-")) {
                    params.get(key).add(args[i++]);
                }
                i--;
            }
        }
        bootstrapServer = String.join(",", params.getOrDefault("--bootstrap-server", new ArrayList<>()));
        topic = String.join(",", params.getOrDefault("--topic", new ArrayList<>()));
        fileDest = String.join(",", params.getOrDefault("--file-dest", new ArrayList<>()));
        if ("".equals(bootstrapServer) || "".equals(topic) || "".equals(fileDest)) {
            System.err.println("invalid parameter, follow --bootstrap-server <bootstrap-server> --topic <topic> --file-dest <file-dest>");
            return;
        }
        System.out.println("params = " + params);
        System.out.println("######### start to parse file");
        List<String> jsons = parseData(fileDest);
        System.out.println("######### parse done");

        Properties producerProps = new Properties();
        producerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println(String.format("######### start to send message: total[%s]", jsons.size()));
        int i = 0;
        for (String json : jsons) {
            System.out.println("send: " + json);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, json);
            producer.send(record);
            i++;
            System.out.println(processBar(i, json.length()));
        }
        System.out.println("######### finish send messages");

    }

    public static String processBar(int pos, int total) {
        int percent = pos * 10 / total;
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < 10; ++i) {
            if (percent-- > 0) {
                bar.append("***");
            } else {
                bar.append("---");
            }
        }
        bar.append("]  " + pos * 100 / total + "%");
        return bar.toString();
    }

    public static List<String> parseData(String fileDest) throws IOException {
        List<String> jsons = new ArrayList<>();
        try (FileReader fr = new FileReader(fileDest);
             BufferedReader bufr = new BufferedReader(fr)) {
            String line = null;
            while ((line = bufr.readLine()) != null) {
                jsons.add(line);
            }
        }
        return jsons;
    }
}

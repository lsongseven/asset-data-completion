package com.envisioniot.enos.asset.data.completion;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @Author liang.song lsongseven@gmail.com
 * @Date 2020/11/12 20:03
 */
public class AssetDataCompletion {
    public static void main(String[] args) throws IOException {

        String bootStrapServer, topic, fileDest;
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
        bootStrapServer = String.join(",", params.getOrDefault("--bootstrap-server", new ArrayList<>()));
        topic = String.join(",", params.getOrDefault("--topic", new ArrayList<>()));
        fileDest = String.join(",", params.getOrDefault("--file-dest", new ArrayList<>()));
        if ("".equals(bootStrapServer) || "".equals(topic) || "".equals(fileDest)) {
            System.err.println("invalid parameter, follow --bootstrap-server <bootstrap-server> --topic <topic> --file-dest <file-dest>");
            return;
        }
        System.out.println("\n params = " + params);
        System.out.println("\n start to parse file");
        List<String> jsons = parseData(fileDest);
        System.out.println("\n parse done");

        Properties producerProps = new Properties();
        producerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println(String.format("\n start to send message: total[%s]", jsons.size()));
        int i = 0;
        for (String json : jsons) {
            System.out.println("\n send: " + json);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, json);
            producer.send(record);
            i++;
            System.out.println(processBar(i, json.length()));
        }
        System.out.println("\n finish send messages");

    }

    public static String processBar(int pos, int total) {
        int percent = pos * 10 / total;
        StringBuilder bar = new StringBuilder("\n [");
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
        try (InputStream inputStream = new FileInputStream(fileDest)) {

        }
        return null;
    }
}

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
        int i = 0, currentPercent = 0;
        for (String json : jsons) {
            ProducerRecord<String, String> record = new ProducerRecord<>("enos-iam.resource.operate.event",  json);
            producer.send(record);
            i++;
            if (i * 100 / jsons.size() > currentPercent) {
                currentPercent++;
                System.out.println(processBar(i, jsons.size()));
            }
        }
        producer.flush();
        producer.close();
        System.out.println("######### finish send messages");

    }

    public static String processBar(int pos, int total) {
        int percent = pos * 10 / total;
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < 10; ++i) {
            if (percent-- > 0) {
                bar.append("**");
            } else {
                bar.append("--");
            }
        }
        bar.append("]  " + pos * 100 / total + "%");
        return bar.toString();
    }

    public static List<String> parseData(String fileDest) throws IOException {
        List<String> results = new ArrayList<>();
        List<String> jsons = new ArrayList<>();
        Map<String, String> treeId2OrgId = new HashMap<>();
        try (FileReader fr = new FileReader(fileDest);
             BufferedReader bufr = new BufferedReader(fr)) {
            String line = null;
            while ((line = bufr.readLine()) != null) {
                if (line.length() > 1) {
                    line = line.substring(1, line.length() - 1);
                }

                Map map = JsonUtil.fromJson(line, Map.class);
                ExternalResourceEvent event = new ExternalResourceEvent();
                event.actions = Arrays.asList("read", "write", "control");
                event.displayOrder = 0;
                event.operationType = "create";
                event.resourceType = "asset_node";
                event.externalId = (String) map.getOrDefault("instanceId", "");
                event.organizationId = (String) map.getOrDefault("__OU", "");
                event.parentExternalId = (String) map.getOrDefault("treeId", "");
                event.name = new HashMap<>();
                event.name.put("default", (String) map.getOrDefault("nameOFdefault", ""));
                event.name.put("en_US", (String) map.getOrDefault("nameOFen_US", ""));
                event.name.put("zh_CN", (String) map.getOrDefault("nameOFzh_CN", ""));

                treeId2OrgId.put((String) map.getOrDefault("treeId", ""), (String) map.getOrDefault("__OU", ""));

                jsons.add(JsonUtil.toJson(event));
                event.parentExternalId = "assets.virtual." + event.organizationId;
                jsons.add(JsonUtil.toJson(event));
            }
            for (Map.Entry<String, String> entry : treeId2OrgId.entrySet()) {
                ExternalResourceEvent event = new ExternalResourceEvent();
                event.actions = Arrays.asList("read", "write", "control");
                event.displayOrder = 0;
                event.operationType = "create";
                event.resourceType = "asset_node";
                event.externalId = entry.getKey();
                event.organizationId = entry.getValue();
                event.parentExternalId = null;
                event.name = new HashMap<>();
//                event.name.put("default", (String) map.getOrDefault("nameOFdefault", ""));
//                event.name.put("en_US", (String) map.getOrDefault("nameOFen_US", ""));
//                event.name.put("zh_CN", (String) map.getOrDefault("nameOFzh_CN", ""));
                results.add(JsonUtil.toJson(event));
            }
            results.addAll(jsons);
            for(String result:results){
                System.out.println(result);
            }
        }
        return results;
    }
}

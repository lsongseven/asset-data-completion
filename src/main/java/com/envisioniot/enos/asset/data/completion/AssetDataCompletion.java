package com.envisioniot.enos.asset.data.completion;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.envisioniot.enos.iam.api.dto.ContextUser;
import com.envisioniot.enos.iam.api.dto.Organization;
import com.envisioniot.enos.iam.api.enums.CertificationState;
import com.envisioniot.enos.iam.api.enums.OrganizationState;
import com.envisioniot.enos.iam.api.enums.OrganizationType;
import com.envisioniot.enos.iam.api.request.organization.OrganizationListRequest;
import com.envisioniot.enos.iam.api.response.organization.OrganizationListResponse;
import com.envisioniot.enos.iam.api.rpc.OrganizationService;
import com.envisioniot.enos.model_service.share.ITSLInstanceService;
import com.envisioniot.enos.model_service.share.data.auth.Audit;
import com.envisioniot.enos.model_service.share.data.i18n.TSLStringI18n;
import com.envisioniot.enos.model_service.share.data.response.MSPageRsp;
import com.envisioniot.enos.model_service.share.data.tsl.TSLInstance;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @Author liang.song lsongseven@gmail.com
 * @Date 2020/11/12 20:03
 */
public class AssetDataCompletion {

    private static final int PAGE_SIZE = 1000;

    public static void main(String[] args) throws IOException {

        String bootstrapServer, topic, fileDest, zkServer;
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
        zkServer = String.join(",", params.getOrDefault("--zk-server", new ArrayList<>()));
        if ("".equals(bootstrapServer) || "".equals(topic) || ("".equals(fileDest) && "".equals(zkServer))) {
            System.err.println("invalid parameter, follow --bootstrap-server <bootstrap-server> --topic <topic> [--zk-server <zk-server>] [--file-dest <file-dest>]\n" +
                    "\n" +
                    "Option\t\t\t\t\t\tDescription\n" +
                    "--bootstrap-server\t\t\tThe address of any kafka broker \n" +
                    "--topic\t\t\t\t\t\tThe kafka topic name, here is 'enos-iam.resource.operate.event'\n" +
                    "--zk-server\t\t\t\t\t(Optional) The address of zookeeper which is used as the dubbo register\n" +
                    "--file-dest\t\t\t\t\t(Optional) The path of file which contains neo4j data of the asset tree");
            return;
        }
        System.out.println("params = " + params);

        List<String> messages = new ArrayList<>();
        if (!"".equals(fileDest)) {
            System.out.println("######### start to parse file");
            List<String> assetTreeMethods = parseData(fileDest);
            messages.addAll(assetTreeMethods);
            System.out.println("######### parse done");
        }
        if (!"".equals(zkServer)) {
            System.out.println("######### start to invoke model service rpc");
            List<String> virtualAssetMessages = getAllVirtualMessages(zkServer);
            messages.addAll(virtualAssetMessages);
            System.out.println("######### rpc done");
        }

        Properties producerProps = new Properties();
        producerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println(String.format("######### start to send message: total[%s]", messages.size()));
        int i = 0, currentPercent = 0;
        for (String json : messages) {
            ProducerRecord<String, String> record = new ProducerRecord<>("enos-iam.resource.operate.event", json);
            producer.send(record);
            i++;
            if (i * 100 / messages.size() > currentPercent) {
                currentPercent++;
                System.out.println(processBar(i, messages.size()));
            }
        }
        producer.flush();
        producer.close();
        System.out.println("######### finish send messages");
        System.exit(1);
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
        bar.append("]  " + pos * 100 / total + "% (do not terminate the process until you see 'finish send message'");
        return bar.toString();
    }

    @SneakyThrows
    public static List<String> parseData(String fileDest) throws IOException {
        List<String> results = new ArrayList<>();
        List<String> jsons = new ArrayList<>();
        Map<String, String> treeId2OrgId = new HashMap<>();
        try (FileReader fr = new FileReader(fileDest);
             BufferedReader bufr = new BufferedReader(fr)) {
            String line = null;
            while ((line = bufr.readLine()) != null) {
                try {
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
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    System.err.println("parse error: " + line);
                }
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
                event.name.put("default", "default_tree_name");
                event.name.put("en_US", "en_us_tree_name");
                event.name.put("zh_CN", "zh_cn_tree_name");
                results.add(JsonUtil.toJson(event));
            }
            results.addAll(jsons);
            for (String result : results) {
                System.out.println(result);
            }
        }
        return results;
    }

    public static ITSLInstanceService getITSLInstanceService(String zkServer) {
        ReferenceConfig<ITSLInstanceService> reference = new ReferenceConfig<>();
        reference.setTimeout(300000);
        reference.setApplication(new ApplicationConfig("enos-iam-asset-data-completion"));
        reference.setRegistry(new RegistryConfig(zkServer));
        reference.setInterface(ITSLInstanceService.class);
        return reference.get();
    }

    public static OrganizationService getOrganizationService(String zkServer) {
        ReferenceConfig<OrganizationService> reference = new ReferenceConfig<>();
        reference.setTimeout(300000);
        reference.setApplication(new ApplicationConfig("enos-iam-asset-data-completion"));
        reference.setRegistry(new RegistryConfig(zkServer));
        reference.setInterface(OrganizationService.class);
        return reference.get();
    }

    public static List<String> getAllVirtualMessages(String zkServer) {
        List<String> results = Collections.synchronizedList(new ArrayList<>());

        OrganizationService organizationService = getOrganizationService(zkServer);
        OrganizationListRequest organizationListRequest = new OrganizationListRequest(OrganizationState.NORMAL, CertificationState.UNDEFINED, null, OrganizationType.UNDEFINED, null, null);
        OrganizationListResponse organizationListResponse = organizationService.list(organizationListRequest, new ContextUser("0", "0", null));
        List<String> orgIds = organizationListResponse.getOrganizations().stream().map(Organization::getId).collect(Collectors.toList());

        batchProcess(orgIds, zkServer, results);
        return results;
    }


    @SneakyThrows
    private static void batchProcess(List<String> orgIds, String zkServer, List<String> results) {
        long start = System.currentTimeMillis();
        int corePoolSize = 300;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("rpc-pool-%d").build();
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(corePoolSize, corePoolSize, 10L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(500), threadFactory);
        ITSLInstanceService itslInstanceService = getITSLInstanceService(zkServer);
        List<CompletableFuture<Void>> pageFutures = new ArrayList<>();
//        futureTasks("o16056681415101814",zkServer,results,executorService,itslInstanceService,pageFutures);
        for (String orgId : orgIds) {
            futureTasks(orgId, zkServer, results, executorService, itslInstanceService, pageFutures);
        }
        System.out.println("waiting for futures done: ");
        CompletableFuture.allOf(pageFutures.toArray(new CompletableFuture[0])).get();
        System.out.println("futures all done: elapsed time " + (System.currentTimeMillis() - start) + " ms");

    }

    private static void futureTasks(String orgId, String zkServer, List<String> results, ThreadPoolExecutor executorService, ITSLInstanceService itslInstanceService, List<CompletableFuture<Void>> pageFutures) {
        MSPageRsp<TSLInstance> tslInstanceMSPageRsp = itslInstanceService.queryTSLInstanceByFilter(orgId, null, PAGE_SIZE, 1, new Audit());
        int total = tslInstanceMSPageRsp.getData().getTotalRecordCount();
        int currentPage = 0;
        while ((currentPage++ * PAGE_SIZE) <= total) {
            int finalCurrentPage = currentPage;
            CompletableFuture<Void> pageFuture = CompletableFuture.supplyAsync(() -> {
//                ITSLInstanceService itslInstanceService1 = getITSLInstanceService(zkServer);
                MSPageRsp<TSLInstance> tslInstanceMSPageRsp1 = itslInstanceService.queryTSLInstanceByFilter(orgId, null, PAGE_SIZE, finalCurrentPage, new Audit());
                List<String> ouAssetMessages = tslInstanceMSPageRsp1.getData().getRecord().stream().map(tslInstance -> {
                    ExternalResourceEvent event = new ExternalResourceEvent();
                    event.actions = Arrays.asList("read", "write", "control");
                    event.displayOrder = 0;
                    event.operationType = "create";
                    event.resourceType = "asset_node";
                    event.externalId = tslInstance.getTslInstanceId();
                    event.organizationId = orgId;
                    event.parentExternalId = "assets.virtual." + orgId;
                    TSLStringI18n tslInstanceName = tslInstance.getTslInstanceName();
                    event.name = new HashMap<>();
                    event.name.put("default", tslInstanceName.getDefaultValue());
                    event.name.put("en_US", tslInstanceName.getLocalizedValue(Locale.US.toString()));
                    event.name.put("zh_CN", tslInstanceName.getLocalizedValue(Locale.SIMPLIFIED_CHINESE.toString()));
                    return event;
                }).map(JsonUtil::toJson).collect(Collectors.toList());
                return ouAssetMessages;
            }, executorService).thenAcceptAsync(messages -> {
                results.addAll(messages);
                System.out.println(String.format("%s: ou, %s, message size: %s", Thread.currentThread().getName(), orgId, results.size()));
            }, executorService);
            pageFutures.add(pageFuture);
        }
    }
}

package com.sentinel_api;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
public class InfraConnectivityCheck implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(InfraConnectivityCheck.class);

    private final KafkaAdmin kafkaAdmin;

    @Value("${sentinel.clickhouse.url}")
    private String chUrl;
    @Value("${sentinel.clickhouse.username}")
    private String chUser;
    @Value("${sentinel.clickhouse.password}")
    private String chPassword;

    public InfraConnectivityCheck(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public void run(String... args) {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = admin.describeCluster();
            log.info("KAFKA OK: clusterId={}, brokers={}",
                    cluster.clusterId().get(10, TimeUnit.SECONDS),
                    cluster.nodes().get(10, TimeUnit.SECONDS).size());
        } catch (Exception e) {
            log.error("KAFKA connection FAILED", e);
        }

        try (Connection conn = DriverManager.getConnection(chUrl, chUser, chPassword);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            rs.next();
            log.info("CLICKHOUSE OK: version={}", rs.getString(1));
        } catch (Exception e) {
            log.error("CLICKHOUSE connection FAILED", e);
        }
    }
}
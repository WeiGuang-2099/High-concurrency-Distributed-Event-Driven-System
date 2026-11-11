package com.auction.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gateway integration tests run with the config server switched off, so they cannot see
 * what config-repo/ would add. Values served by the config server override the gateway's
 * own application.yml: one {@code spring.cloud.gateway.discovery.locator.enabled: true}
 * there would publish every service as /{service-id}/** again while every other test stays
 * green. This test reads the files directly and keeps routing out of them.
 */
class ConfigRepoLeavesRoutingAloneTest {

    @Test
    void configRepo_definesNoGatewayRoutingProperties() throws IOException {
        List<Path> files = configRepoFiles();
        assertThat(files).as("yaml files in config-repo").isNotEmpty();

        for (Path file : files) {
            YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
            yaml.setResources(new FileSystemResource(file));
            Properties properties = yaml.getObject();

            assertThat(properties.stringPropertyNames())
                    .as("properties in %s", file.getFileName())
                    .noneMatch(name -> name.startsWith("spring.cloud.gateway"));
        }
    }

    private static List<Path> configRepoFiles() throws IOException {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("config-repo");
            if (Files.isDirectory(candidate)) {
                try (Stream<Path> entries = Files.list(candidate)) {
                    return entries
                            .filter(path -> {
                                String name = path.getFileName().toString();
                                return name.endsWith(".yml") || name.endsWith(".yaml");
                            })
                            .toList();
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Cannot locate config-repo above " + Paths.get("").toAbsolutePath());
    }
}

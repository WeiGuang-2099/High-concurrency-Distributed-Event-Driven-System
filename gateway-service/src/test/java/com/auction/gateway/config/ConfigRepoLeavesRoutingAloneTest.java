package com.auction.gateway.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Values served by the config server override the gateway's own application.yml. One
 * {@code spring.cloud.gateway.discovery.locator.enabled: true} in config-repo/ would publish
 * every service as /{service-id}/** again. This test reads the files and keeps routing out
 * of them, so the route table has one home.
 *
 * <p>GatewayAllowListIT imports the same files and checks the resulting route table, which
 * is the stronger check. This one is the fast one, and it names the offending key.
 *
 * <p>The first version of this test was blind to exactly the case it was written for. It
 * looked at {@code Properties.stringPropertyNames()}, which leaves out every key whose value
 * is not a String, and YAML {@code true} is loaded as a Boolean. Reverting the fix by hand
 * showed it stayed green, hence the second test below.
 */
class ConfigRepoLeavesRoutingAloneTest {

    @Test
    void configRepo_definesNoGatewayRoutingProperties() throws IOException {
        List<Path> files = configRepoFiles();
        assertThat(files).as("yaml files in config-repo").isNotEmpty();

        for (Path file : files) {
            assertThat(gatewayKeysIn(file)).as("spring.cloud.gateway.* keys in %s", file.getFileName()).isEmpty();
        }
    }

    @Test
    void theCheckSeesGatewayKeys_whateverTheTypeOfTheirValue(@TempDir Path dir) throws IOException {
        Path booleanValue = Files.writeString(dir.resolve("boolean.yml"), """
                spring:
                  cloud:
                    gateway:
                      discovery:
                        locator:
                          enabled: true
                """);
        Path numberValue = Files.writeString(dir.resolve("number.yml"), """
                spring:
                  cloud:
                    gateway:
                      httpclient:
                        connect-timeout: 1000
                """);
        Path routeList = Files.writeString(dir.resolve("routes.yml"), """
                spring:
                  cloud:
                    gateway:
                      routes:
                        - id: everything
                          uri: lb://ticket-service
                          predicates:
                            - Path=/api/tickets/**
                """);
        Path unrelated = Files.writeString(dir.resolve("unrelated.yml"), """
                spring:
                  cloud:
                    loadbalancer:
                      retry:
                        enabled: true
                jwt:
                  issuer: auction-platform
                """);

        // The config server also serves .properties files, and files in sub-folders.
        Path propertiesFile = Files.writeString(dir.resolve("gateway-service.properties"),
                "spring.cloud.gateway.discovery.locator.enabled=true\n");
        Path nested = Files.createDirectories(dir.resolve("some-label")).resolve("gateway-service.yml");
        Files.writeString(nested, """
                spring.cloud.gateway.discovery.locator.enabled: true
                """);

        assertThat(gatewayKeysIn(booleanValue)).containsExactly("spring.cloud.gateway.discovery.locator.enabled");
        assertThat(gatewayKeysIn(numberValue)).containsExactly("spring.cloud.gateway.httpclient.connect-timeout");
        assertThat(gatewayKeysIn(routeList)).contains("spring.cloud.gateway.routes[0].uri");
        assertThat(gatewayKeysIn(propertiesFile)).containsExactly("spring.cloud.gateway.discovery.locator.enabled");
        assertThat(gatewayKeysIn(unrelated)).isEmpty();
        assertThat(configFilesUnder(dir)).as("files found, sub-folders included")
                .contains(propertiesFile, nested)
                .hasSize(6);
    }

    /** Every flattened key under spring.cloud.gateway, whatever type its value has. */
    private static List<String> gatewayKeysIn(Path file) throws IOException {
        Properties properties;
        if (file.getFileName().toString().endsWith(".properties")) {
            properties = new Properties();
            try (InputStream in = Files.newInputStream(file)) {
                properties.load(in);
            }
        } else {
            YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
            yaml.setResources(new FileSystemResource(file));
            properties = yaml.getObject();
        }
        // keySet(), not stringPropertyNames(): the latter drops keys with non-String values.
        return properties.keySet().stream()
                .map(String::valueOf)
                .filter(name -> name.startsWith("spring.cloud.gateway"))
                .sorted()
                .toList();
    }

    private static List<Path> configRepoFiles() throws IOException {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("config-repo");
            if (Files.isDirectory(candidate)) {
                return configFilesUnder(candidate);
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Cannot locate config-repo above " + Paths.get("").toAbsolutePath());
    }

    /** Every .yml, .yaml and .properties file below the directory, at any depth. */
    private static List<Path> configFilesUnder(Path root) throws IOException {
        try (Stream<Path> entries = Files.walk(root)) {
            return entries
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.endsWith(".yml") || name.endsWith(".yaml") || name.endsWith(".properties");
                    })
                    .toList();
        }
    }
}

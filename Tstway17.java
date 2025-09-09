package com.sathya.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}


package com.sathya.gateway.config;

import com.sathya.gateway.filter.JwtAuthFilter;
import com.sathya.gateway.filter.NodeLoggingFilter;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RouteConfig {

    private final JwtAuthFilter jwtAuthFilter;
    private final NodeLoggingFilter nodeLoggingFilter;

    public RouteConfig(JwtAuthFilter jwtAuthFilter, NodeLoggingFilter nodeLoggingFilter) {
        this.jwtAuthFilter = jwtAuthFilter;
        this.nodeLoggingFilter = nodeLoggingFilter;
    }

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
            .route("admin-api", r -> r.path("/api/**")
                .filters(f -> f
                    .filter(jwtAuthFilter)
                    .filter(nodeLoggingFilter)
                    .retry(config -> config
                        .retries(3)
                        .statuses(org.springframework.http.HttpStatus.BAD_GATEWAY,
                                  org.springframework.http.HttpStatus.GATEWAY_TIMEOUT,
                                  org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE)
                        .methods(org.springframework.http.HttpMethod.GET,
                                 org.springframework.http.HttpMethod.POST)
                        .backoff(org.springframework.cloud.gateway.filter.factory.RetryGatewayFilterFactory.BackoffConfig
                            .exponential(java.time.Duration.ofMillis(100),
                                         java.time.Duration.ofSeconds(2),
                                         2, true))
                    )
                )
                .uri("http://kafka-admin"))
            .build();
    }
}


package com.sathya.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class JwtAuthFilter implements GatewayFilter {

    @Value("${jwt.secret}")
    private String secret;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String token = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (token == null || !token.startsWith("Bearer ")) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        try {
            Claims claims = Jwts.parser()
                .setSigningKey(secret.getBytes())
                .parseClaimsJws(token.replace("Bearer ", ""))
                .getBody();

            // Optional: pass user info downstream
            exchange.getRequest().mutate()
                .header("X-User", claims.getSubject())
                .build();

        } catch (Exception e) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        return chain.filter(exchange);
    }
}


package com.sathya.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.URI;

@Component
public class NodeLoggingFilter implements GatewayFilter {

    private static final Logger logger = LoggerFactory.getLogger(NodeLoggingFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        URI targetUri = exchange.getRequest().getURI();
        logger.info("Routing request to node: {}", targetUri.getHost());
        return chain.filter(exchange);
    }
}


server.port=8080
spring.application.name=kafka-admin-gateway

# JWT secret (on-prem only, no cloud vaults)
jwt.secret=your-256-bit-secret-key

# Retry configuration
spring.cloud.gateway.retry.enabled=true
spring.cloud.gateway.retry.retries=3
spring.cloud.gateway.retry.statuses=BAD_GATEWAY,GATEWAY_TIMEOUT,SERVICE_UNAVAILABLE
spring.cloud.gateway.retry.methods=GET,POST
spring.cloud.gateway.retry.backoff.firstBackoff=100ms
spring.cloud.gateway.retry.backoff.maxBackoff=2s
spring.cloud.gateway.retry.backoff.factor=2
spring.cloud.gateway.retry.backoff.basedOnPreviousValue=true


  <project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.sathya</groupId>
  <artifactId>kafka-admin-gateway</artifactId>
  <version>1.0.0</version>

  <properties>
    <java.version>17</java.version>
    <spring.boot.version>3.1.4</spring.boot.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-gateway</artifactId>
    </dependency>
    <dependency>
      <groupId>io.jsonwebtoken</groupId>
      <artifactId>jjwt-api</artifactId>
      <version>0.11.5</version>
    </dependency>
    <dependency>
      <groupId>io.jsonwebtoken</groupId>
      <artifactId>jjwt-impl</artifactId>
      <version>0.11.5</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>io.jsonwebtoken</groupId>
      <artifactId>jjwt-jackson</artifactId>
      <version>0.11.5</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.10.1</version>
        <configuration>
          <source>17</source>
          <target>17</target>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>

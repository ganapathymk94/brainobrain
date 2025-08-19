@SpringBootApplication
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}

@Configuration
public class RouteConfig {

    @Autowired
    private JwtAuthFilter jwtAuthFilter;

    @Autowired
    private NodeLoggingFilter nodeLoggingFilter;

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
            .route("admin-api", r -> r.path("/api/**")
                .filters(f -> f
                    .filter(jwtAuthFilter)
                    .filter(nodeLoggingFilter)
                    .retry(config -> config
                        .retries(3)
                        .statuses(HttpStatus.BAD_GATEWAY, HttpStatus.GATEWAY_TIMEOUT, HttpStatus.SERVICE_UNAVAILABLE)
                        .methods(HttpMethod.GET, HttpMethod.POST)
                        .backoff(BackoffConfig.exponential(Duration.ofMillis(100), Duration.ofSeconds(2), 2, true))
                    )
                )
                .uri("http://kafka-admin"))
            .build();
    }
}


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
                .setSigningKey(secret.getBytes("UTF-8"))
                .parseClaimsJws(token.replace("Bearer ", ""))
                .getBody();
        } catch (Exception e) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        return chain.filter(exchange);
    }
}

@Component
public class NodeLoggingFilter implements GatewayFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        URI targetUri = exchange.getRequest().getURI();
        String host = targetUri.getHost();
        System.out.println("Routing request to node: " + host);
        return chain.filter(exchange);
    }
}

server.port=8080
jwt.secret=your-secret-key-here
spring.application.name=kafka-admin-gateway


<project xmlns="http://maven.apache.org/POM/4.0.0" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.kafka.gateway</groupId>
    <artifactId>kafka-admin-gateway</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>1.8</java.version>
        <spring.boot.version>2.3.12.RELEASE</spring.boot.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-gateway</artifactId>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt</artifactId>
            <version>0.9.1</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

package uk.ac.ed.acp.cw2.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

@Configuration
@EnableScheduling
public class AppConfig {
    @Bean
    public RuntimeEnvironment CurrentRuntimeEnvironment() {
        return RuntimeEnvironment.getEnvironment();
    }
}

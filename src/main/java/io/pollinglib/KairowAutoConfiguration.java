package io.pollinglib;

import javax.sql.DataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(name = "kairow.enabled", havingValue = "true", matchIfMissing = true)
public class KairowAutoConfiguration {
    @Bean
    KairowScheduler kairowScheduler(ConfigurableListableBeanFactory beans, DataSource source, Environment env) {
        return new KairowScheduler(beans, source, env);
    }
}

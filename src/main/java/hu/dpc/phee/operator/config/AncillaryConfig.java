package hu.dpc.phee.operator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AncillaryConfig {

    @Value("${reliability.events-timestamps-dump-enabled}")
    public String enableTimestampsDump;
}

package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {

    static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);

    private final RestTemplate restTemplate;
    private static final String INCENTIVE_URL = "http://localhost:8080/incentive";

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public float getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_URL, transaction, Incentive.class);
            if (incentive != null) {
                logger.info("Incentive received: {}", incentive.getAmount());
                return incentive.getAmount();
            }
        } catch (Exception e) {
            logger.warn("Failed to fetch incentive: {}", e.getMessage());
        }
        return 0f;
    }
}

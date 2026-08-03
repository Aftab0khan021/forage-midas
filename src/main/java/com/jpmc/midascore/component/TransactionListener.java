package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    public TransactionListener(UserRepository userRepository,
                               TransactionRepository transactionRepository,
                               IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void onMessage(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate senderId
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return;
        }

        // Validate recipientId
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender {}: balance={}, amount={}",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Fetch incentive from the API
        float incentiveAmount = incentiveService.getIncentive(transaction);

        // Persist the transaction record (with incentive)
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(record);

        // Update balances:
        // - sender pays transaction amount (not incentive)
        // - recipient receives transaction amount + incentive
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        userRepository.save(sender);
        userRepository.save(recipient);

        logger.info("Transaction processed: {} -> {} : amount={}, incentive={}",
                sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);
        logger.info("New balances — {}: {}, {}: {}",
                sender.getName(), sender.getBalance(),
                recipient.getName(), recipient.getBalance());
    }
}

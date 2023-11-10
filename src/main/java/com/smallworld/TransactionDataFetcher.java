package com.smallworld;

import com.smallworld.data.Transaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionDataFetcher {
	
	private List<Transaction> transactions;

	public TransactionDataFetcher() {
        try {
            String filePath = "/transactions.json";
            Path absolutePath = Paths.get(filePath).toAbsolutePath();
            System.out.println("Absolute Path: " + absolutePath);

            ObjectMapper objectMapper = new ObjectMapper();
            byte[] fileContent = Files.readAllBytes(absolutePath);
            transactions = Arrays.asList(objectMapper.readValue(fileContent, Transaction[].class));
        } catch (IOException e) {
            handleException("Error loading transactions from JSON file", e);
            transactions = Collections.emptyList();
        }
	}

    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount() {
    	try {
            return transactions.stream().mapToDouble(Transaction::getAmount).sum();
        } catch (Exception e) {
            handleException("Error calculating total transaction amount", e);
            return 0.0;
        }
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderFullName) {
    	try {
            return transactions.stream()
                    .filter(transaction -> senderFullName.equals(transaction.getSenderFullName()))
                    .mapToDouble(Transaction::getAmount)
                    .sum();
        } catch (Exception e) {
            handleException("Error calculating total transaction amount sent by " + senderFullName, e);
            return 0.0;
        }
    }

    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount() {
    	try {
            if (transactions.isEmpty()) {
                throw new IllegalStateException("No transactions available");
            }
            return transactions.stream().mapToDouble(Transaction::getAmount).max().orElseThrow();
        } catch (Exception e) {
            handleException("Error calculating max transaction amount", e);
            return 0.0;
        }
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() {
    	try {
            return transactions.stream()
                    .flatMap(transaction -> Stream.of(transaction.getSenderFullName(), transaction.getBeneficiaryFullName()))
                    .distinct()
                    .count();
        } catch (Exception e) {
            handleException("Error counting unique clients", e);
            return 0;
        }
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) {
    	try {
            return transactions.stream()
                    .anyMatch(transaction ->
                            (clientFullName.equals(transaction.getSenderFullName()) ||
                                    clientFullName.equals(transaction.getBeneficiaryFullName()))
                                    && !transaction.isIssueSolved());
        } catch (Exception e) {
            handleException("Error checking open compliance issues for " + clientFullName, e);
            return false;
        }
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, Transaction> getTransactionsByBeneficiaryName() {
    	try {
            return transactions.stream()
                    .collect(Collectors.toMap(Transaction::getBeneficiaryFullName, Function.identity()));
        } catch (Exception e) {
            handleException("Error getting transactions by beneficiary name", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() {
    	try {
            return transactions.stream()
                    .filter(transaction -> !transaction.isIssueSolved() && transaction.getIssueId() != null)
                    .map(Transaction::getIssueId)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            handleException("Error getting unsolved issue ids", e);
            return Collections.emptySet();
        }
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() {
    	try {
            return transactions.stream()
                    .filter(Transaction::isIssueSolved)
                    .map(Transaction::getIssueMessage)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            handleException("Error getting all solved issue messages", e);
            return Collections.emptyList();
        }
    }

    /**
     * Returns the 3 transactions with the highest amount sorted by amount descending
     */
    public List<Transaction> getTop3TransactionsByAmount() {
    	try {
            if (transactions.size() < 3) {
                throw new IllegalArgumentException("Insufficient transactions for top 3");
            }
            return transactions.stream()
                    .sorted(Comparator.comparingDouble(Transaction::getAmount).reversed())
                    .limit(3)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            handleException("Error getting top 3 transactions by amount", e);
            return Collections.emptyList();
        }
    }

    /**
     * Returns the senderFullName of the sender with the most total sent amount
     */
    public Optional<String> getTopSender() {
    	try {
            if (transactions.isEmpty()) {
                return Optional.empty();
            }
            return transactions.stream()
                    .collect(Collectors.groupingBy(Transaction::getSenderFullName,
                            Collectors.summingDouble(Transaction::getAmount)))
                    .entrySet().stream()
                    .max(Comparator.comparingDouble(Map.Entry::getValue))
                    .map(Map.Entry::getKey);
        } catch (Exception e) {
            handleException("Error getting top sender", e);
            return Optional.empty();
        }
    }
    
    private void handleException(String message, Exception e) {
        System.err.println(message);
        e.printStackTrace();
    }

}

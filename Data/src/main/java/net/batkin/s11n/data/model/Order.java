package net.batkin.s11n.data.model;

public class Order {

    private String ticker;
    private long quantity;
    private int accountNumber;
    private String strategy;

    public Order(String ticker, long quantity, int accountNumber, String strategy) {
        this.ticker = ticker;
        this.quantity = quantity;
        this.accountNumber = accountNumber;
        this.strategy = strategy;
    }

    public String getTicker() {
        return ticker;
    }

    public long getQuantity() {
        return quantity;
    }

    public int getAccountNumber() {
        return accountNumber;
    }

    public String getStrategy() {
        return strategy;
    }

    @Override
    public String toString() {
        return "Order{" +
                "ticker='" + ticker + '\'' +
                ", quantity=" + quantity +
                ", accountNumber=" + accountNumber +
                ", strategy='" + strategy + '\'' +
                '}';
    }
}

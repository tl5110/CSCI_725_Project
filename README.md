# CSCI_725_Project
## Four Datasets
### /baseline
- These datasets contain data that has clean, balanced activity to get our first throughput and latency numbers
- Balanced Data for Sanity Checks. Each scenario already includes its own customers and merchants.
- Additional Info:
    ```
    scenario: baseline
    customers: 40000 rows
    merchants: 2000 rows
    accounts: 10000 rows
    transactions: 100000 rows
    seed: 7252025
    ```
### /edgecases
- These datasets contain data that help with correctness checks. They include frozen and closed accounts as well as a few duplicate transfer ids to test if our unique rules can catch them.
- Edgecase-Handling Data. Includes attempted overdrafts to verify your guards. Expect some operations to be rejected by your unique transfer id constraint or index.
- Additional Info:
    ```
    scenario: edgecases
    customers: 40000 rows
    merchants: 2000 rows
    accounts: 5000 rows
    transactions: 50000 rows
    contents: frozen and closed accounts and a few duplicate transfer ids to test idempotency
    seed: 7252025
    ```
### /hotspot
- These datasets contain data that are heavily skewed where most writes hit a small set of accounts to test contention
- Skewed Access Pattern. The read or write mix is the same as baseline, but the hot keys create contention.
- Additional Info:
    ```
    scenario: hotspot
    customers: 40000 rows
    merchants: 2000 rows
    accounts: 10000 rows
    transactions: 100000 rows
    skew: 80 percent of writes hit 20 percent of accounts
    seed: 7252025
    ```
### /payday
- These datasets contain data that gives strong time spikes to test burst behavior and recovery
- Spike Heavy Time Series. During spikes there are more deposits and transfers, which stresses write paths and recent history queries.
- Additional Info:
    ```
    scenario: payday
    customers: 40000 rows
    merchants: 2000 rows
    accounts: 10000 rows
    transactions: 100000 rows
    time spikes: two day spikes with five times arrival rate
    seed: 7252025
    ```

#### Note: 
> CSV Files --> PostgreSQL\
> JSON Lines --> MongoDB


## Application Functions
- **Open Account**: create a new account for a customer with status open and balance 0.
- **Deposit**: add money to an account. Append a transaction row and update the account balance inside one unit of work.
- **Withdraw**: take money out if funds plus overdraft allow it. Append a transaction row and update the balance in one unit of work.
- **Transfer**: move money between two accounts or to a verified merchant. Before completing the transfer, the system validates that the sender’s account has sufficient funds, the receiver or merchant exists, and that the merchant (if involved) is active. If valid, it creates two transaction rows (a debit and a credit) with the same transfer_id, commits them together as one atomic operation, and rejects duplicates by transfer_id.
- **Get Balance**: read the current account balance.
- **View Recent Transactions**: read the last N transactions for an account, ordered by timestamp descending.
- **Close/Freeze Account**: change status to closed or frozen. Do not delete rows. You want an audit trail.
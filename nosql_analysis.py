import random
import time
import inspect
import sys
import csv
import json
import threading
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from statistics import mean

from nosql_implementation import (
    connectToMongoDB,
    createBankingCollections,
    createBankingIndexes,
    dropBankingCollections,
    loadSampleData,
    openAccount,
    deposit,
    withdraw,
    transfer,
    getBalance,
    viewRecentTransactions,
)

# -------------------------------------------------------------------------------------------------
# Config / Defaults
# -------------------------------------------------------------------------------------------------

bankingApplications = [
    ('deposit', 0.20),
    ('withdraw', 0.20),
    ('transfer', 0.20),
    ('getBalance', 0.20),
    ('viewRecentTransactions', 0.20),
]
concurrencyLvls = [1, 5, 10, 20, 50]
defaultRun = 10
testingScenarios = ['baseline', 'edgecases', 'hotspot', 'payday']


def countDbLines(fn):
    """
    Counts lines of code inside a function that directly touch the database.
    
    ::param fn:: function to inspect
    ::return:: count of lines containing 'db.'
    """
    src = inspect.getsource(fn)
    return sum(1 for line in src.splitlines() if 'db.' in line)


# -------------------------------------------------------------------------------------------------
# Development Metrics
# -------------------------------------------------------------------------------------------------

applicationMetrics = {
    'openAccount': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(openAccount),
        'schema_or_index_changes': []
    },
    'deposit': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(deposit),
        'schema_or_index_changes': []
    },
    'withdraw': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(withdraw),
        'schema_or_index_changes': []
    },
    'transfer': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(transfer),
        'schema_or_index_changes': [
            'transfer_id_unique index',
            'account_recent_history index'
        ]
    },
    'getBalance': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(getBalance),
        'schema_or_index_changes': []
    },
    'viewRecentTransactions': {
        'time_minutes': None,
        'lines_touching_db': countDbLines(viewRecentTransactions),
        'schema_or_index_changes': ['account_recent_history index']
    }
}


def recordFeatureTime(featureName, minutesSpent):
    """
    Records the development time for a feature so we can compare SQL vs NoSQL effort.

    ::param featureName:: name of the feature (keys from DEVELOPMENT_METRICS)
    ::param minutesSpent:: minutes spent building the feature
    ::return:: None
    """
    if featureName in applicationMetrics:
        applicationMetrics[featureName]['time_minutes'] = minutesSpent


def summarizeDevelopmentMetrics():
    """
    Returns a snapshot of development-effort metrics with DB line counts pre-filled.
    
    ::return:: dictionary of development metrics
    """
    return applicationMetrics


# -------------------------------------------------------------------------------------------------
# Benchmark Helpers
# -------------------------------------------------------------------------------------------------

def pickOperation(rng, operationMix):
    """
    Picks a single operation name based on weighted mix.

    ::param rng:: random generator
    ::param operationMix:: list of tuples (operation, weight)
    """
    names = [item[0] for item in operationMix]
    weights = [item[1] for item in operationMix]
    return rng.choices(names, weights=weights, k=1)[0]


def buildAccountPools(db, sampleSize=50):
    """
    Builds cached account and merchant lists for sampling.

    ::param db:: active database object
    ::param sampleSize:: max documents to sample
    """
    accounts = list(db.Accounts.find(
        {'status': 'open'},
        {'_id': 1, 'customer_id': 1}
    ).limit(sampleSize))
    merchants = list(db.Merchants.find({}, {'_id': 1}).limit(sampleSize))
    return accounts, merchants


def chooseAccountPair(rng, accounts):
    """
    Chooses two different accounts for transfer operations.

    ::param rng:: random generator
    ::param accounts:: list of account docs
    """
    if len(accounts) < 2:
        return None, None
    first = rng.choice(accounts)
    second = rng.choice(accounts)
    while second['_id'] == first['_id'] and len(accounts) > 1:
        second = rng.choice(accounts)
    return first, second


def runOperations(db, opName, accounts, merchants, rng):
    """
    Executes a single operation from the mix.

    ::param db:: active database object
    ::param opName:: name of the operation
    ::param accounts:: cached accounts list
    ::param merchants:: cached merchants list
    ::param rng:: random generator
    ::return:: True if operation succeeded, False otherwise
    """
    if not accounts:
        return False

    if opName == 'deposit':
        acc = rng.choice(accounts)
        amount = rng.randint(500, 2500)
        return deposit(db, accountId=acc['_id'], amount=amount, logOutput=False)

    if opName == 'withdraw':
        acc = rng.choice(accounts)
        amount = rng.randint(200, 1500)
        return withdraw(db, accountId=acc['_id'], amount=amount, logOutput=False)

    if opName == 'transfer':
        fromAcc, toAcc = chooseAccountPair(rng, accounts)
        if not fromAcc or not toAcc:
            return False
        amount = rng.randint(200, 1500)
        merchantId = rng.choice(merchants)['_id'] if merchants else None
        return transfer(
            db,
            fromAccId=fromAcc['_id'],
            toAccId=toAcc['_id'],
            amount=amount,
            merchantId=merchantId,
            channel='online',
            logOutput=False
        )

    if opName == 'getBalance':
        acc = rng.choice(accounts)
        balance = getBalance(db, accountId=acc['_id'], customerId=acc.get('customer_id'), logOutput=False)
        return balance is not None

    if opName == 'viewRecentTransactions':
        acc = rng.choice(accounts)
        txns = viewRecentTransactions(db, accountId=acc['_id'], logOutput=False)
        return True if txns is not None else False

    return False


def computePercentile(values, pct):
    """
    Computes percentile for a list of values.

    ::param values:: list of numeric values
    ::param pct:: percentile between 0 and 1
    """
    if not values:
        return 0
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return ordered[f]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


def runNosqlBenchmark(db, concurrencyLevels=None, operationMix=None, runSeconds=defaultRun):
    """
    Runs a fixed read/write mix against MongoDB and records latency/throughput.

    ::param db:: active database object
    ::param concurrencyLevels:: list of concurrency values (defaults to [1,5,10,20,50])
    ::param operationMix:: list of (operation, weight) tuples
    ::param runSeconds:: seconds to run each level
    """
    levels = concurrencyLevels or concurrencyLvls
    mix = operationMix or bankingApplications
    accounts, merchants = buildAccountPools(db)

    results = []
    for level in levels:
        latencies = []
        counts = defaultdict(int)
        successes = 0
        failures = 0

        lock = threading.Lock()

        def worker():
            nonlocal successes, failures
            rng = random.Random()
            stopAt = time.perf_counter() + runSeconds
            while time.perf_counter() < stopAt:
                op = pickOperation(rng, mix)
                started = time.perf_counter()
                ok = runOperations(db, op, accounts, merchants, rng)
                elapsed = (time.perf_counter() - started) * 1000
                with lock:
                    latencies.append(elapsed)
                    counts[op] += 1
                    if ok:
                        successes += 1
                    else:
                        failures += 1

        with ThreadPoolExecutor(max_workers=level) as pool:
            for _ in range(level):
                pool.submit(worker)

        duration = runSeconds
        avg_latency = mean(latencies) if latencies else 0
        p95_latency = computePercentile(latencies, 0.95)
        throughput = successes / duration if duration else 0

        results.append({
            'concurrency': level,
            'throughput_ops_per_sec': round(throughput, 2),
            'avg_latency_ms': round(avg_latency, 2),
            'p95_latency_ms': round(p95_latency, 2),
            'successes': successes,
            'failures': failures,
            'operation_counts': dict(counts)
        })

    return results


def resetAndLoadScenario(db, scenario):
    """
    Resets MongoDB collections and loads a scenario dataset for benchmarking.

    ::param db:: active database object
    ::param scenario:: one of baseline, edgecases, hotspot, payday
    """
    dropBankingCollections(db)
    createBankingCollections(db)
    createBankingIndexes(db)
    loadSampleData(db, scenario)


def runScenarioBenchmarks(db, scenarios=None):
    """
    Runs benchmarks for each scenario and returns a mapping of scenario->results.

    ::param db:: active database object
    ::param scenarios:: list of scenario names
    """
    scenarioList = scenarios or testingScenarios
    allResults = {}
    for scenario in scenarioList:
        resetAndLoadScenario(db, scenario)
        perfResults = runNosqlBenchmark(db)
        allResults[scenario] = perfResults
    return allResults


def writeDetailedCSV(perfByScenario, path):
    """
    Writes flattened benchmark results for plotting.

    ::param perfByScenario:: dictionary mapping scenario to result rows
    ::param path:: filesystem path to write CSV
    """
    fieldnames = [
        'scenario',
        'concurrency',
        'throughput_ops_per_sec',
        'avg_latency_ms',
        'p95_latency_ms',
        'successes',
        'failures',
        'operation_counts'
    ]
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for scenario, rows in perfByScenario.items():
            for row in rows:
                writer.writerow({
                    'scenario': scenario,
                    'concurrency': row.get('concurrency'),
                    'throughput_ops_per_sec': row.get('throughput_ops_per_sec'),
                    'avg_latency_ms': row.get('avg_latency_ms'),
                    'p95_latency_ms': row.get('p95_latency_ms'),
                    'successes': row.get('successes'),
                    'failures': row.get('failures'),
                    'operation_counts': json.dumps(row.get('operation_counts', {}))
                })


def writeSummaryCSV(perfByScenario, path):
    """
    Writes per-scenario rollups for quick comparison.

    ::param perfByScenario:: dictionary mapping scenario to result rows
    ::param path:: filesystem path to write CSV
    """
    fieldnames = [
        'scenario',
        'avg_latency_ms',
        'p95_latency_ms',
        'max_throughput_ops_per_sec'
    ]
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for scenario, rows in perfByScenario.items():
            if not rows:
                continue
            avg_lat = sum(r['avg_latency_ms'] for r in rows) / len(rows)
            p95_lat = sum(r['p95_latency_ms'] for r in rows) / len(rows)
            max_tps = max(r['throughput_ops_per_sec'] for r in rows)
            writer.writerow({
                'scenario': scenario,
                'avg_latency_ms': round(avg_lat, 2),
                'p95_latency_ms': round(p95_lat, 2),
                'max_throughput_ops_per_sec': max_tps
            })


def plot(perfByScenario):
    """
    Tries to plot latency/throughput charts if matplotlib is available.

    ::param perfByScenario:: dictionary mapping scenario to result rows
    """
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not installed; skipping PNG charts.")
        return

    # Latency chart
    plt.figure(figsize=(10, 6))
    for scenario, rows in perfByScenario.items():
        xs = [r['concurrency'] for r in rows]
        ys = [r['avg_latency_ms'] for r in rows]
        plt.plot(xs, ys, marker='o', label=f"{scenario} avg latency")
    plt.xlabel("Concurrency")
    plt.ylabel("Avg latency (ms)")
    plt.title("MongoDB latency by scenario")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("nosql_latency.png")

    # Throughput chart
    plt.figure(figsize=(10, 6))
    for scenario, rows in perfByScenario.items():
        xs = [r['concurrency'] for r in rows]
        ys = [r['throughput_ops_per_sec'] for r in rows]
        plt.plot(xs, ys, marker='o', label=f"{scenario} tps")
    plt.xlabel("Concurrency")
    plt.ylabel("Throughput (ops/sec)")
    plt.title("MongoDB throughput by scenario")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("nosql_throughput.png")


def main():
    """
    Entry point to run benchmarks across scenarios and emit reports.
    """
    db, client = connectToMongoDB()
    scenarios = sys.argv[1:] if len(sys.argv) > 1 else testingScenarios

    metrics = summarizeDevelopmentMetrics()
    perfByScenario = runScenarioBenchmarks(db, scenarios=scenarios)

    print("\n=== Development Effort Metrics (NoSQL) ===")
    for feature, meta in metrics.items():
        print(f"{feature}: time_minutes={meta['time_minutes']}, db_lines={meta['lines_touching_db']}, schema/index changes={meta['schema_or_index_changes']}")

    for scenario, perfResults in perfByScenario.items():
        print(f"\n=== Performance Metrics (NoSQL) - {scenario} ===")
        for result in perfResults:
            print(result)

    resultsPath = Path("nosql_benchmark_results.csv")
    summaryPath = Path("nosql_benchmark_summary.csv")
    writeDetailedCSV(perfByScenario, resultsPath)
    writeSummaryCSV(perfByScenario, summaryPath)
    print(f"\nWrote detailed results to {resultsPath}")
    print(f"Wrote summary to {summaryPath}")

    plot(perfByScenario)

    client.close()


if __name__ == "__main__":
    main()

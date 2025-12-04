import time
import random
import threading
import csv
import json
from pathlib import Path
from statistics import mean
from concurrent.futures import ThreadPoolExecutor
import psycopg as psql
import matplotlib.pyplot as plt

from sql_implementation import (
    createTables,
    loadData,
    dropTables,
    OpenAccount,
    closeAccount,
    getBalance,
    Transfer,
    Withdraw,
    Deposit,
    viewRecentTransactions,
)


###############################################################
# CONFIG
###############################################################

bankingApplications = [
    ('deposit', 0.20),
    ('withdraw', 0.20),
    ('transfer', 0.20),
    ('getBalance', 0.20),
    ('viewRecentTransactions', 0.20),
]

concurrencyLvls = [1, 5, 10, 20, 50]
defaultRunSeconds = 10
testingScenarios = ['baseline', 'edgecases', 'hotspot', 'payday']

sampleAccountLimit = 200

DB_PARAMS = {
    "dbname": "CSCI_725_Project",
    "host": "127.0.0.1",
    "user": "root",
    "password": "MYsql990001161"
}


###############################################################
# CONNECTION
###############################################################

def new_conn():
    return psql.connect(**DB_PARAMS)


###############################################################
# ACCOUNT POOL / OPS
###############################################################

def build_account_pool(conn, sample_size):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT account_id 
            FROM accounts 
            WHERE status = 'open'
            LIMIT %s
        """, (sample_size,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def choose_account_pair(rng, accounts):
    if len(accounts) < 2:
        return None, None
    a, b = rng.sample(accounts, 2)
    return a, b


def pickOperation(rng, mix):
    names = [op for op, w in mix]
    weights = [w for op, w in mix]
    return rng.choices(names, weights=weights, k=1)[0]


def run_sql_operation(conn, op_name, accounts, rng):
    start = time.perf_counter()
    try:
        if op_name == 'deposit':
            acc = rng.choice(accounts)
            Deposit(conn, acc, rng.randint(100, 2000))

        elif op_name == 'withdraw':
            acc = rng.choice(accounts)
            Withdraw(conn, acc, rng.randint(50, 1500))

        elif op_name == 'transfer':
            from_acc, to_acc = choose_account_pair(rng, accounts)
            if not from_acc or not to_acc:
                raise RuntimeError("Not enough accounts for transfer")
            Transfer(conn, from_acc, to_acc, rng.randint(50, 1000))

        elif op_name == 'getBalance':
            acc = rng.choice(accounts)
            bal = getBalance(conn, acc)
            if bal is None:
                raise RuntimeError("getBalance returned None")

        elif op_name == 'viewRecentTransactions':
            acc = rng.choice(accounts)
            tx = viewRecentTransactions(conn, acc)
            if tx is None:
                raise RuntimeError("viewRecentTransactions returned None")

        else:
            raise ValueError("Unknown op")

        elapsed = (time.perf_counter() - start) * 1000
        return True, elapsed

    except Exception:
        try:
            conn.rollback()
        except:
            pass
        elapsed = (time.perf_counter() - start) * 1000
        return False, elapsed


###############################################################
# SQL BENCHMARK CORE
###############################################################

def run_sql_benchmark(concurrency_levels, operation_mix, account_pool, run_seconds):
    results = []

    if len(account_pool) == 0:
        raise RuntimeError("ERROR: Account pool is empty — benchmark cannot run")

    for level in concurrency_levels:
        latencies = []
        successes = 0
        failures = 0
        lock = threading.Lock()

        # FIX: Pass explicit account pool into workers
        accounts = list(account_pool)

        def worker():
            nonlocal successes, failures
            conn = new_conn()
            rng = random.Random()
            stop_at = time.perf_counter() + run_seconds

            try:
                while time.perf_counter() < stop_at:
                    op = pickOperation(rng, operation_mix)
                    ok, elapsed = run_sql_operation(conn, op, accounts, rng)
                    with lock:
                        latencies.append(elapsed)
                        if ok:
                            successes += 1
                        else:
                            failures += 1
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=level) as pool:
            [pool.submit(worker) for _ in range(level)]

        avg_latency = mean(latencies)
        p95 = sorted(latencies)[int(0.95 * (len(latencies) - 1))]
        throughput = successes / run_seconds

        results.append({
            "concurrency": level,
            "avg_latency_ms": round(avg_latency, 3),
            "p95_latency_ms": round(p95, 3),
            "throughput_ops_per_sec": round(throughput, 3),
            "successes": successes,
            "failures": failures
        })

    return results


###############################################################
# SCENARIO DEFINITIONS
###############################################################

def scenario_operation_mix(name):
    if name == "baseline":
        return bankingApplications

    if name == "edgecases":
        return [
            ('withdraw', 0.35),
            ('transfer', 0.35),
            ('deposit', 0.10),
            ('getBalance', 0.10),
            ('viewRecentTransactions', 0.10),
        ]

    if name == "hotspot":
        return [
            ('deposit', 0.15),
            ('withdraw', 0.15),
            ('transfer', 0.45),
            ('getBalance', 0.15),
            ('viewRecentTransactions', 0.10),
        ]

    if name == "payday":
        return [
            ('deposit', 0.55),
            ('withdraw', 0.10),
            ('transfer', 0.15),
            ('getBalance', 0.10),
            ('viewRecentTransactions', 0.10),
        ]

    raise ValueError("Unknown scenario")


def scenario_account_pool(conn, scenario):
    if scenario == "hotspot":
        return build_account_pool(conn, 20)
    if scenario == "edgecases":
        return build_account_pool(conn, 150)
    if scenario == "payday":
        return build_account_pool(conn, 300)
    return build_account_pool(conn, sampleAccountLimit)


###############################################################
# PLOTTING
###############################################################

def plot_sql_all_scenarios(perfByScenario):

    # LATENCY
    plt.figure(figsize=(10, 6))
    for scenario, rows in perfByScenario.items():
        xs = [r["concurrency"] for r in rows]
        ys = [r["avg_latency_ms"] for r in rows]
        plt.plot(xs, ys, marker="o", label=f"{scenario} avg latency")
    plt.xlabel("Concurrency")
    plt.ylabel("Avg latency (ms)")
    plt.title("PostgreSQL latency by scenario")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("sql_latency_by_scenario.png")

    # THROUGHPUT
    plt.figure(figsize=(10, 6))
    for scenario, rows in perfByScenario.items():
        xs = [r["concurrency"] for r in rows]
        ys = [r["throughput_ops_per_sec"] for r in rows]
        plt.plot(xs, ys, marker="o", label=f"{scenario} throughput")
    plt.xlabel("Concurrency")
    plt.ylabel("Throughput (ops/sec)")
    plt.title("PostgreSQL throughput by scenario")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("sql_throughput_by_scenario.png")


###############################################################
# RUN ALL SCENARIOS
###############################################################

def run_all_scenarios():
    all_results = {}

    for scenario in testingScenarios:
        print("\n=== RUNNING SCENARIO:", scenario.upper(), "===")

        op_mix = scenario_operation_mix(scenario)

        with new_conn() as conn:
            account_pool = scenario_account_pool(conn, scenario)

        if not account_pool:
            raise RuntimeError(f"ERROR: Scenario {scenario} has NO accounts loaded!")

        results = run_sql_benchmark(
            concurrencyLvls, op_mix, account_pool, defaultRunSeconds
        )

        all_results[scenario] = results

    return all_results


###############################################################
# MAIN
###############################################################

def main():

    # Load DB once (same data across scenarios)
    with new_conn() as conn:
        dropTables(conn)
        createTables(conn)
        loadData(conn)

    # Run scenario benchmarks
    results = run_all_scenarios()

    # Plot combined graphs
    plot_sql_all_scenarios(results)

    print("\n=== SQL SCENARIO BENCHMARK COMPLETE ===")
    print("Generated: sql_latency_by_scenario.png")
    print("Generated: sql_throughput_by_scenario.png")


if __name__ == "__main__":
    main()

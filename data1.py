# === Full Excel Sheet Reconciliation Assignment ===
import time
import random
import numpy as np
import pandas as pd
from itertools import product, combinations
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from deap import base, creator, tools, algorithms
from datetime import datetime

# === Task 1.1 & 1.2 : Load, Examine, Clean, Standardize ===
# Load the Excel files
df_targets = pd.read_excel(r"C:\Users\PMLS\Desktop\financial-data-parser\data\sample\Customer_Ledger_Entries_FULL.xlsx")
df_transactions = pd.read_excel(r"C:\Users\PMLS\Desktop\financial-data-parser\data\sample\KH_Bank.XLSX")
print("Transactions shape:", df_transactions.shape)
print("Targets shape:", df_targets.shape)

# Check and handle missing values
print("Missing in transactions:\n", df_transactions.isnull().sum())
print("Missing in targets:\n", df_targets.isnull().sum())
df_transactions['Statement.Entry.Amount.Value'] = df_transactions['Statement.Entry.Amount.Value'].fillna(0)
df_targets['Original Amount'] = df_targets['Original Amount'].fillna(0)
df_transactions['Statement.Entry.EntryDetails.TransactionDetails.AdditionalTransactionInformation'] = df_transactions['Statement.Entry.EntryDetails.TransactionDetails.AdditionalTransactionInformation'].fillna('No Description')
df_targets['Description'] = df_targets['Description'].fillna('No Description')
df_transactions['Statement.Entry.Amount.Currency'] = df_transactions['Statement.Entry.Amount.Currency'].fillna('Unknown')
df_targets['Currency Code'] = df_targets['Currency Code'].fillna('Unknown')
df_transactions = df_transactions.dropna(thresh=int(0.5 * len(df_transactions.columns)))
df_targets = df_targets.dropna(thresh=int(0.5 * len(df_targets.columns)))

# Save cleaned data
df_transactions.to_excel('cleaned_transactions.xlsx', index=False)
df_targets.to_excel('cleaned_targets.xlsx', index=False)

# Clean and standardize amounts
def clean_amount(value):
    if isinstance(value, str):
        value = value.replace('$', '').replace('€', '').replace('HUF', '').replace('EUR', '').replace(',', '')
    try:
        return round(float(value), 2)
    except:
        return 0.0
df_transactions['Statement.Entry.Amount.Value'] = df_transactions['Statement.Entry.Amount.Value'].apply(clean_amount)
df_targets['Original Amount'] = df_targets['Original Amount'].apply(clean_amount)

# Standardize to common currency (HUF to EUR)
exchange_rate_huf_to_eur = 400
df_transactions['Standardized Amount'] = df_transactions.apply(
    lambda row: row['Statement.Entry.Amount.Value'] / exchange_rate_huf_to_eur if row['Statement.Entry.Amount.Currency'] == 'HUF' else row['Statement.Entry.Amount.Value'], axis=1)
df_targets['Standardized Amount'] = df_targets.apply(
    lambda row: row['Original Amount'] / exchange_rate_huf_to_eur if row['Currency Code'] == 'HUF' else row['Original Amount'], axis=1)

# Print and save standardized data
print("Cleaned transactions sample:\n", df_transactions[['Statement.Entry.Amount.Value', 'Statement.Entry.Amount.Currency']].head())
print("Cleaned targets sample:\n", df_targets[['Original Amount', 'Currency Code']].head())
df_transactions.to_excel('standardized_transactions.xlsx', index=False)
df_targets.to_excel('standardized_targets.xlsx', index=False)

# Create unique IDs
df_transactions['Transaction_ID'] = (df_transactions['Statement.Entry.EntryReference'].fillna('').astype(str) + '_' + df_transactions.index.astype(str))
df_targets['Target_ID'] = ['TGT_' + str(i+1).zfill(4) for i in range(len(df_targets))]
print("Transactions with IDs:\n", df_transactions[['Transaction_ID', 'Statement.Entry.Amount.Value']].head())
print("Targets with IDs:\n", df_targets[['Target_ID', 'Original Amount']].head())
df_transactions.to_excel('prepared_transactions.xlsx', index=False)
df_targets.to_excel('prepared_targets.xlsx', index=False)

# === Task 2.1: Direct Matching ===
matches = {}
for tx_id, tx_row in df_transactions.iterrows():
    tx_amount = tx_row['Statement.Entry.Amount.Value']
    tx_currency = tx_row['Statement.Entry.Amount.Currency']
    for tgt_id, tgt_row in df_targets.iterrows():
        tgt_amount = tgt_row['Original Amount']
        tgt_currency = tgt_row['Currency Code']
        if (tx_amount == tgt_amount and (tx_currency == tgt_currency or tx_currency == 'Unknown' or tgt_currency == 'Unknown')):
            matches[tx_id] = tgt_id
            break
match_results = pd.DataFrame(list(matches.items()), columns=['Transaction_ID', 'Target_ID'])
print("Direct Matches:\n", match_results)
match_results.to_excel('direct_matches.xlsx', index=False)
print("Match results saved to 'direct_matches.xlsx'")
matched_details = pd.merge(df_transactions[['Transaction_ID', 'Statement.Entry.Amount.Value', 'Statement.Entry.Amount.Currency']],
                          df_targets[['Target_ID', 'Original Amount', 'Currency Code']],
                          left_on='Transaction_ID', right_on='Target_ID', how='right')
print("Matched Details:\n", matched_details)

# === Task 2.2: Subset Sum Brute Force ===
transaction_amounts = df_transactions['Statement.Entry.Amount.Value'].tolist()
target_amounts = df_targets['Original Amount'].tolist()
def find_subset_sums(transactions, targets, max_combination_size=5):
    matches = {}
    for target_idx, target_amount in enumerate(targets):
        target_id = df_targets.iloc[target_idx]['Target_ID']
        for r in range(1, min(max_combination_size, len(transactions)) + 1):
            for combo in combinations(enumerate(transactions), r):
                combo_sum = sum(amount for _, amount in combo)
                if abs(combo_sum - target_amount) < 0.01:
                    tx_indices = [i[0] for i in combo]
                    tx_ids = [df_transactions.iloc[i]['Transaction_ID'] for i in tx_indices]
                    matches[target_id] = {'Target_Amount': target_amount, 'Transaction_IDs': tx_ids, 'Sum': combo_sum}
                    break
            if target_id in matches:
                break
    return matches
subset_matches = find_subset_sums(transaction_amounts, target_amounts)
subset_results = pd.DataFrame([{'Target_ID': k, 'Target_Amount': v['Target_Amount'], 
                               'Transaction_IDs': ', '.join(v['Transaction_IDs']), 'Sum': v['Sum']}
                              for k, v in subset_matches.items()])
print("Subset Sum Matches:\n", subset_results)
subset_results.to_excel('subset_sum_matches.xlsx', index=False)
print("Subset sum results saved to 'subset_sum_matches.xlsx'")

# Performance test for Task 2.2
for size in [10, 50, 100]:
    sample_transactions = transaction_amounts[:size]
    sample_targets = target_amounts[:size]
    start_time = time.time()
    _ = find_subset_sums(sample_transactions, sample_targets)
    end_time = time.time()
    print(f"Time for dataset size {size}: {end_time - start_time:.4f} seconds")

# === Task 3.1: Feature Engineering ===
tfidf = TfidfVectorizer(stop_words='english', max_features=500)
transactions = df_transactions[['Transaction_ID', 'Statement.Entry.Amount.Value', 
                               'Statement.Entry.Amount.Currency', 
                               'Statement.Entry.EntryDetails.TransactionDetails.AdditionalTransactionInformation']]
targets = df_targets[['Target_ID', 'Original Amount', 'Currency Code', 'Description']]
pairs = list(product(transactions.itertuples(), targets.itertuples()))
features = []
for tx, tgt in pairs:
    amount_diff = abs(tx._2 - tgt._2)
    currency_match = 1 if (tx._3 == tgt._3 or tx._3 == 'Unknown' or tgt._3 == 'Unknown') else 0
    tx_desc = tx._4 if pd.notnull(tx._4) else ''
    tgt_desc = tgt._4 if pd.notnull(tgt._4) else ''
    similarity = (tfidf.fit_transform([tx_desc, tgt_desc]) * tfidf.fit_transform([tx_desc, tgt_desc]).T).A[0, 1] if tx_desc or tgt_desc else 0.0
    features.append({'Transaction_ID': tx._1, 'Target_ID': tgt._1, 'Amount_Difference': amount_diff,
                     'Currency_Match': currency_match, 'Description_Similarity': similarity})
feature_df = pd.DataFrame(features)
feature_df.to_excel('feature_engineered_data.xlsx', index=False)
print("Feature engineering complete. Saved to 'feature_engineered_data.xlsx'")

# === Task 3.2: Dynamic Programming Enhancement ===
def subset_sum_dp(transactions, targets):
    matches = {}
    for target_idx, target_amount in enumerate(targets):
        target_id = df_targets.iloc[target_idx]['Target_ID']
        n = len(transactions)
        dp = [[False] * (int(target_amount * 100) + 1) for _ in range(n + 1)]
        for i in range(n + 1):
            dp[i][0] = True
        for i in range(1, n + 1):
            for j in range(1, int(target_amount * 100) + 1):
                if int(transactions[i-1] * 100) <= j:
                    dp[i][j] = dp[i-1][j] or dp[i-1][j - int(transactions[i-1] * 100)]
        if dp[n][int(target_amount * 100)]:
            tx_ids = []
            remaining = int(target_amount * 100)
            for i in range(n, 0, -1):
                if remaining >= int(transactions[i-1] * 100) and dp[i-1][remaining - int(transactions[i-1] * 100)]:
                    tx_ids.append(df_transactions.iloc[i-1]['Transaction_ID'])
                    remaining -= int(transactions[i-1] * 100)
            matches[target_id] = {'Target_Amount': target_amount, 'Transaction_IDs': tx_ids, 'Sum': remaining / 100.0}
    return matches
dp_matches = subset_sum_dp(transaction_amounts, target_amounts)
dp_results = pd.DataFrame([{'Target_ID': k, 'Target_Amount': v['Target_Amount'], 
                           'Transaction_IDs': ', '.join(v['Transaction_IDs']), 'Sum': v['Sum']}
                          for k, v in dp_matches.items()])
dp_results.to_excel('dp_subset_sum_matches.xlsx', index=False)
print("Dynamic programming subset sum complete. Saved to 'dp_subset_sum_matches.xlsx'")

# === Task 3.3: Machine Learning Models (optional) ===
feature_df = pd.read_excel('feature_engineered_data.xlsx')
direct_matches = pd.read_excel('direct_matches.xlsx')
match_pairs = set(zip(direct_matches['Transaction_ID'], direct_matches['Target_ID']))
feature_df['Is_Match'] = feature_df.apply(lambda row: 1 if (row['Transaction_ID'], row['Target_ID']) in match_pairs else 0, axis=1)
X = feature_df[['Amount_Difference', 'Currency_Match', 'Description_Similarity']]
y = feature_df['Is_Match']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print("Classification Report:\n", classification_report(y_test, y_pred))
feature_df['Match_Likelihood'] = model.predict_proba(X)[:, 1]
feature_df.to_excel('ml_predictions.xlsx', index=False)
print("Machine learning predictions saved to 'ml_predictions.xlsx'")

# === Task 4.1: Genetic Algorithm for Subset Selection ===
creator.create("FitnessMin", base.Fitness, weights=(-1.0,))
creator.create("Individual", list, fitness=creator.FitnessMin)
toolbox = base.Toolbox()
toolbox.register("attr_bool", random.randint, 0, 1)
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_bool, n=len(transaction_amounts))
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
def evaluate(individual, target):
    selected_amounts = [amt for idx, amt in enumerate(transaction_amounts) if individual[idx] == 1]
    return 1000000 if not selected_amounts else abs(sum(selected_amounts) - target),
toolbox.register("evaluate", evaluate)
toolbox.register("mate", tools.cxTwoPoint)
toolbox.register("mutate", tools.mutFlipBit, indpb=0.05)
toolbox.register("select", tools.selTournament, tournsize=3)
ga_matches = {}
pop_size, ngen = 50, 20
for target_idx, target_amount in enumerate(target_amounts):
    target_id = df_targets.iloc[target_idx]['Target_ID']
    population = toolbox.population(n=pop_size)
    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("min", np.min)
    population, logbook = algorithms.eaSimple(population, toolbox, cxpb=0.5, mutpb=0.2, ngen=ngen, stats=stats, verbose=False)
    best_ind = tools.selBest(population, k=1)[0]
    selected_indices = [i for i, x in enumerate(best_ind) if x == 1]
    tx_ids = [df_transactions.iloc[i]['Transaction_ID'] for i in selected_indices]
    total_sum = sum(transaction_amounts[i] for i in selected_indices)
    ga_matches[target_id] = {'Target_Amount': target_amount, 'Transaction_IDs': tx_ids, 'Sum': total_sum}
ga_results = pd.DataFrame([{'Target_ID': k, 'Target_Amount': v['Target_Amount'], 
                           'Transaction_IDs': ', '.join(v['Transaction_IDs']), 'Sum': v['Sum']}
                          for k, v in ga_matches.items()])
ga_results.to_excel('ga_subset_matches.xlsx', index=False)
print("Genetic algorithm subset selection complete. Saved to 'ga_subset_matches.xlsx'")
for target_id, match in ga_matches.items():
    print(f"Target {target_id}: Sum = {match['Sum']}, Transactions = {match['Transaction_IDs']}")

# === Task 5.1: Benchmarking ===
# Direct Matching Function
def direct_matching(transactions, targets):
    matches = {}
    for tx_id, tx_row in transactions.iterrows():
        tx_amount = tx_row['Statement.Entry.Amount.Value']
        tx_currency = tx_row['Statement.Entry.Amount.Currency']
        for tgt_id, tgt_row in targets.iterrows():
            tgt_amount = tgt_row['Original Amount']
            tgt_currency = tgt_row['Currency Code']
            if (tx_amount == tgt_amount and (tx_currency == tgt_currency or tx_currency == 'Unknown' or tgt_currency == 'Unknown')):
                matches[tx_id] = tgt_id
                break
    return len(matches)

# Subset Sum Brute Force Function
def find_subset_sums_bench(transactions, targets, max_combination_size=5):
    matches = {}
    for target_idx, target_amount in enumerate(targets):
        for r in range(1, min(max_combination_size, len(transactions)) + 1):
            for combo in combinations(enumerate(transactions), r):
                combo_sum = sum(amount for _, amount in combo)
                if abs(combo_sum - target_amount) < 0.01:
                    break
    return len(matches)

# Dynamic Programming Function
def subset_sum_dp_bench(transactions, targets):
    matches = {}
    for target_idx, target_amount in enumerate(targets):
        n = len(transactions)
        dp = [[False] * (int(target_amount * 100) + 1) for _ in range(n + 1)]
        for i in range(n + 1):
            dp[i][0] = True
        for i in range(1, n + 1):
            for j in range(1, int(target_amount * 100) + 1):
                if int(transactions[i-1] * 100) <= j:
                    dp[i][j] = dp[i-1][j] or dp[i-1][j - int(transactions[i-1] * 100)]
        if dp[n][int(target_amount * 100)]:
            matches[target_idx] = True
    return len(matches)

# Genetic Algorithm Function
def genetic_algorithm_bench(transactions, targets):
    pop = toolbox.population(n=50)
    algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=0.2, ngen=5, verbose=False)
    best = tools.selBest(pop, k=1)[0]
    return sum(1 for x in best if x == 1)

# Benchmarking
sizes = [10, 50, 100]
results = []
for size in sizes:
    sample_transactions = df_transactions['Statement.Entry.Amount.Value'].head(size).tolist()
    sample_targets = df_targets['Original Amount'].head(size).tolist()
    sample_df_transactions = df_transactions.head(size)
    sample_df_targets = df_targets.head(size)
    start_time = time.time()
    direct_result = direct_matching(sample_df_transactions, sample_df_targets)
    direct_time = time.time() - start_time
    start_time = time.time()
    brute_result = find_subset_sums_bench(sample_transactions, sample_targets)
    brute_time = time.time() - start_time
    start_time = time.time()
    dp_result = subset_sum_dp_bench(sample_transactions, sample_targets)
    dp_time = time.time() - start_time
    start_time = time.time()
    ga_result = genetic_algorithm_bench(sample_transactions, sample_targets[:1])
    ga_time = time.time() - start_time
    results.append({'Size': size, 'Direct_Matching_Time': direct_time, 'Brute_Force_Time': brute_time,
                    'DP_Time': dp_time, 'GA_Time': ga_time, 'Direct_Matches': direct_result,
                    'Brute_Matches': brute_result, 'DP_Matches': dp_result, 'GA_Selections': ga_result})
benchmark_df = pd.DataFrame(results)
benchmark_df.to_excel('benchmark_results.xlsx', index=False)
print("Benchmarking complete. Results saved to 'benchmark_results.xlsx'")
print(benchmark_df)

# === Task 5.2: Visualization and Reporting ===
# Load benchmark results
benchmark_df = pd.read_excel('benchmark_results.xlsx')
plt.style.use('seaborn')  # Optional, install with 'pip install seaborn'
plt.figure(figsize=(12, 6))
methods = ['Direct_Matching_Time', 'Brute_Force_Time', 'DP_Time', 'GA_Time']
for method in methods:
    plt.plot(benchmark_df['Size'], benchmark_df[method], marker='o', label=method.replace('_Time', ''))
plt.xlabel('Dataset Size')
plt.ylabel('Execution Time (seconds)')
plt.title('Performance Comparison of Reconciliation Methods')
plt.legend()
plt.grid(True)
plt.savefig('performance_comparison.png')
plt.show()
plt.figure(figsize=(12, 6))
match_metrics = ['Direct_Matches', 'Brute_Matches', 'DP_Matches', 'GA_Selections']
bar_width = 0.2
index = range(len(benchmark_df['Size']))
for i, metric in enumerate(match_metrics):
    plt.bar([j + i * bar_width for j in index], benchmark_df[metric], bar_width, label=metric.replace('_', ' ').replace('Matches', 'Matches/Selections'))
plt.xlabel('Dataset Size')
plt.ylabel('Number of Matches/Selections')
plt.title('Match/Selection Counts by Method')
plt.xticks([j + bar_width * 1.5 for j in index], benchmark_df['Size'])
plt.legend()
plt.grid(True)
plt.savefig('match_counts.png')
plt.show()
current_time = datetime.now().strftime("%I:%M %p PKT, %A, %B %d, %Y")
report = f"""
Performance Comparison Report
Date: {current_time}

Summary:
- Dataset sizes tested: {benchmark_df['Size'].tolist()}
- Methods compared: Direct Matching, Brute Force, Dynamic Programming, Genetic Algorithm
- Observations:
  - Direct Matching is fastest for small datasets but limited by exact matches.
  - Brute Force scales poorly with size due to exponential complexity.
  - Dynamic Programming offers a balance of speed and accuracy.
  - Genetic Algorithm provides heuristic solutions with variable performance.

Recommendations:
- Use Direct Matching for small, exact-match datasets.
- Apply Dynamic Programming for larger datasets needing exact sums.
- Consider Genetic Algorithm for approximate solutions with large datasets.

Detailed Results:
{benchmark_df.to_string()}
"""
with open('performance_report.txt', 'w', encoding='utf-8') as f:
    f.write(report)
print("Visualization and report generation complete. Files saved as 'performance_comparison.png', 'match_counts.png', and 'performance_report.txt'")

print("All tasks completed successfully!")
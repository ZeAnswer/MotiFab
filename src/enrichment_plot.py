#!/usr/bin/env python3
import argparse
import pandas as pd
import seaborn as sns
import sys
import os
os.environ["MPLCONFIGDIR"] = "/polio/oded/MotiFabEnv/matplotlib_cache"
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate heatmaps and box plots from an enriched summary CSV."
    )
    parser.add_argument("--summary", required=True, help="Path to the enriched summary CSV file.")
    parser.add_argument("--output_dir", default="plots", help="Directory to save all generated plots.")
    parser.add_argument("--significance_threshold", type=float, default=10,
                        help="Threshold for -log10(p_value) to define significance (default: 10, corresponding to p < 1e-10)")
    return parser.parse_args()

def load_summary(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    return df

def preprocess_summary(df):
    # Ensure that 'test_size' and 'injection_rate' columns are numeric.
    if df['injection_rate'].dtype == object:
        df['injection_rate'] = df['injection_rate'].str.rstrip('%').astype(float)
    else:
        df['injection_rate'] = df['injection_rate'].astype(float)
    df['test_size'] = df['test_size'].astype(float)
    
    # Ensure that is_match is boolean.
    if df['is_match'].dtype == object:
        df['is_match'] = df['is_match'].str.lower().map({'true': True, 'false': False})
    else:
        df['is_match'] = df['is_match'].astype(bool)
    
    # Ensure that is_significant is boolean.
    if 'is_significant' in df.columns:
        if df['is_significant'].dtype == object:
            df['is_significant'] = df['is_significant'].str.lower().map({'true': True, 'false': False})
        else:
            df['is_significant'] = df['is_significant'].astype(bool)
    else:
        df['is_significant'] = False
    
    return df

def compute_match_rate_for_column(df, col):
    # Group by test_size and injection_rate
    group = df.groupby(['test_size', 'injection_rate'])
    summary = group[col].agg(['count', 'sum']).reset_index()
    summary.rename(columns={'count': 'total_runs', 'sum': 'match_count'}, inplace=True)
    summary['match_rate'] = (summary['match_count'] / summary['total_runs']) * 100
    pivot = summary.pivot(index='test_size', columns='injection_rate', values='match_rate')
    return pivot

def plot_heatmap(pivot, title, output_file):
    plt.figure(figsize=(10, 8))
    ax = sns.heatmap(pivot, annot=True, fmt=".1f", cmap="viridis", cbar_kws={'label': 'Match Rate (%)'})
    ax.set_title(title)
    ax.set_xlabel("Injection Rate")
    ax.set_ylabel("Test Size")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    print(f"Saved heatmap to {output_file}")

def prepare_boxplot_data(df):
    if 'p_value' not in df.columns:
        print("Error: p_value column not found in summary file.")
        sys.exit(1)
    try:
        df['p_value'] = pd.to_numeric(df['p_value'], errors='coerce')
    except Exception as e:
        print(f"Error converting p_value: {e}")
        sys.exit(1)
    # Replace zeros to avoid log10(0)
    df['p_value'] = df['p_value'].replace(0, 1e-300)
    # Compute -log10(p_value) so that lower p-values appear higher.
    df['neg_log10_p_value'] = -np.log10(df['p_value'])
    return df

def plot_boxplot_for_test_size(df, test_size, out_dir, significance_threshold):
    df_subset = df[df['test_size'] == test_size]
    if df_subset.empty:
        print(f"No data for test_size {test_size}")
        return
    plt.figure(figsize=(8, 6))
    # Using injection_rate as both x and hue (to get distinct colors), then remove legend.
    ax = sns.boxplot(x="injection_rate", y="neg_log10_p_value", data=df_subset,
                     hue="injection_rate", palette="viridis", dodge=False)
    # Remove the hue legend.
    if ax.get_legend() is not None:
        ax.legend_.remove()
    # Draw horizontal line for significance threshold.
    ax.axhline(y=significance_threshold, color='red', linestyle='--')
    # Add a text label near the line.
    ax.text(0.02, significance_threshold + 0.2, "Significance Threshold", color='red', transform=ax.get_yaxis_transform(), fontsize=10)
    ax.set_title(f"Test Size: {test_size}")
    ax.set_xlabel("Injection Rate")
    ax.set_ylabel("-log10(p_value)")
    plt.tight_layout()
    out_path = os.path.join(out_dir, f"boxplot_testsize_{int(test_size)}.png")
    plt.savefig(out_path)
    plt.close()
    print(f"Saved box plot for test_size {test_size} to {out_path}")

def main():
    args = parse_args()
    if not os.path.exists(args.summary):
        print(f"Summary file not found: {args.summary}")
        sys.exit(1)
    df = load_summary(args.summary)
    df = preprocess_summary(df)
    
    # Create output directory if it doesn't exist.
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Compute pivot tables for heatmaps.
    pivot_match = compute_match_rate_for_column(df, 'is_match')
    pivot_signif = compute_match_rate_for_column(df, 'is_significant')
    
    # Plot heatmap for overall match rate.
    heatmap_match_file = os.path.join(args.output_dir, "match_rate_heatmap.png")
    plot_heatmap(pivot_match, "Match Rate by Test Size and Injection Rate", heatmap_match_file)
    
    # Plot heatmap for significant match rate.
    heatmap_signif_file = os.path.join(args.output_dir, "match_rate_heatmap_significant.png")
    plot_heatmap(pivot_signif, "Significant Match Rate by Test Size and Injection Rate", heatmap_signif_file)
    
    # Prepare boxplot data for p_value.
    df = prepare_boxplot_data(df)
    
    # For each unique test size, create a box plot.
    unique_test_sizes = sorted(df['test_size'].unique())
    for ts in unique_test_sizes:
        plot_boxplot_for_test_size(df, ts, args.output_dir, args.significance_threshold)
    
if __name__ == "__main__":
    main()

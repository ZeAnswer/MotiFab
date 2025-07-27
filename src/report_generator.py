import os
import json
from datetime import datetime
import pandas as pd
import numpy as np
from dataset_manager import DatasetManager
from sklearn.linear_model import LogisticRegression


def generate_report(dm: DatasetManager):
    """
    Generate a Markdown report for a MotiFab run, covering detection rates,
    significance, tool/background rankings, match-score distributions, failure/variability,
    and embedding heatmaps, plus extended analyses per user request.
    """
    params = dm.get_report_params()
    
    # --- Unpack report parameters ---
    output_dir      = params["output_dir"]  # USER-ATTENTION: adjust if needed
    report_filename = params["report_filename"]
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, report_filename)

    seq_amounts     = params["seq_amounts"]
    injection_rates = params["injection_rates"]
    n_replicates    = params["n_replicates"]
    tools           = params["tools"]
    backgrounds     = params["backgrounds"]

    threshold       = params.get("threshold", 0.7)  # USER-ATTENTION: set reliability threshold
    HIGH_CONF       = 0.9  # USER-ATTENTION: set high-confidence threshold

    parsed = params["parsed_results"]
    heatmaps = params["heatmaps"]

    # --- Load CSVs ---
    all_df      = pd.read_csv(parsed["all"]["path"])
    matched_df  = pd.read_csv(parsed["matched"]["path"])
    sig_df      = pd.read_csv(parsed["significant"]["path"])

    total_runs = len(seq_amounts) * len(injection_rates) * n_replicates
    runs_per_tool = total_runs  # Each tool is tested on all combinations

    # --- 1. Detection & Significance by Tool ---
    det_counts = matched_df.drop_duplicates(
        subset=["tool","dataset_length","injection_rate","replicate"]
    ).groupby("tool").size()
    sig_counts = sig_df[sig_df.is_match].drop_duplicates(
        subset=["tool","dataset_length","injection_rate","replicate"]
    ).groupby("tool").size()
    det_table = pd.DataFrame({
        "tool": tools,
        "detection_rate": det_counts.reindex(tools, fill_value=0) / runs_per_tool,
        "significant_rate": sig_counts.reindex(tools, fill_value=0) / runs_per_tool
    }).reset_index(drop=True)

    # 2. Threshold Sweet Spots (significant only)
    if not sig_df[sig_df.is_match].empty:
        fr = (
            sig_df[sig_df.is_match]
            .groupby(["tool","injection_rate","dataset_length","replicate"])  # unique runs
            .size().reset_index()
        )
        frac = (
            fr.groupby(["tool","injection_rate","dataset_length"]).size()
            .div(n_replicates).rename("frac_success").reset_index()
        )
        # min dataset_length per injection_rate
        min_len = (
            frac[frac.frac_success >= threshold]
                .groupby("injection_rate")["dataset_length"].min().reset_index()
                .rename(columns={"dataset_length":"min_dataset_length"})
        )
        # min injection_rate per dataset_length
        min_rate = (
            frac[frac.frac_success >= threshold]
                .groupby("dataset_length")["injection_rate"].min().reset_index()
                .rename(columns={"injection_rate":"min_injection_rate"})
        )
    else:
        # No significant matches found
        min_len = pd.DataFrame(columns=["injection_rate", "min_dataset_length"])
        min_rate = pd.DataFrame(columns=["dataset_length", "min_injection_rate"])
        frac = pd.DataFrame(columns=["tool", "injection_rate", "dataset_length", "frac_success"])

    # 3. Tool × Background Ranking (matched significant runs per background)
    rows = []
    for _, r in sig_df[sig_df.is_match].iterrows():  # Only matched significant motifs
        info = json.loads(r.significance or "{}")
        for bg in backgrounds:
            if bg in info and info[bg].get("significant", False):
                # This run was successful for this tool-background combo
                rows.append({
                    "tool": r.tool, 
                    "background": bg, 
                    "dataset_length": r.dataset_length,
                    "injection_rate": r.injection_rate,
                    "replicate": r.replicate,
                    "successful": True
                })

    df_bg = pd.DataFrame(rows)
    
    # Count successful runs per tool-background, then calculate rate
    if not df_bg.empty:
        success_counts = (
            df_bg.drop_duplicates(["tool", "background", "dataset_length", "injection_rate", "replicate"])
            .groupby(["tool", "background"]).size()
        )
        rank_df = pd.DataFrame({
            "tool": [t for t, b in success_counts.index],
            "background": [b for t, b in success_counts.index], 
            "success_rate": success_counts.values / runs_per_tool
        })
    else:
        rank_df = pd.DataFrame(columns=["tool", "background", "success_rate"])

    # Ensure all tool-background combinations are represented
    full_combinations = pd.MultiIndex.from_product([tools, backgrounds], names=['tool', 'background']).to_frame(index=False)
    rank_df = full_combinations.merge(rank_df, on=['tool', 'background'], how='left').fillna({'success_rate': 0})

    # 6. Insignificant Match Patterns
    fp = det_table.assign(insignificant_rate=lambda df: df.detection_rate - df.significant_rate)[["tool","insignificant_rate"]]

    # 9. Parameter-Trend Tables
    # Ensure all injection rates and dataset lengths are represented
    trend_ir_data = []
    for ir in injection_rates:
        subset = matched_df[matched_df.injection_rate == ir]
        # Count unique (dataset_length, replicate, tool) combinations for this injection_rate
        unique_detections = subset.drop_duplicates(["dataset_length","replicate","tool"]).shape[0] if not subset.empty else 0
        # Total possible detections for this injection_rate across all dataset_lengths, replicates, and tools
        possible_detections = len(seq_amounts) * n_replicates * len(tools)
        trend_ir_data.append({"injection_rate": ir, "detection_rate": unique_detections / possible_detections if possible_detections > 0 else 0})
    trend_ir = pd.DataFrame(trend_ir_data)
    
    trend_dl_data = []
    for dl in seq_amounts:
        subset = matched_df[matched_df.dataset_length == dl]
        # Count unique (injection_rate, replicate, tool) combinations for this dataset_length
        unique_detections = subset.drop_duplicates(["injection_rate","replicate","tool"]).shape[0] if not subset.empty else 0
        # Total possible detections for this dataset_length across all injection_rates, replicates, and tools
        possible_detections = len(injection_rates) * n_replicates * len(tools)
        trend_dl_data.append({"dataset_length": int(dl), "detection_rate": f"{unique_detections / possible_detections:.2%}" if possible_detections > 0 else "0.00%"})
    trend_dl = pd.DataFrame(trend_dl_data)


    # 11. Motif Diversity
    total_cons = all_df.motif_consensus.nunique() if not all_df.empty else 0
    matched_cons = matched_df[matched_df.is_match].motif_consensus.nunique() if not matched_df[matched_df.is_match].empty else 0
    
    # Count insignificant matches (matched but not significant in any background)
    if not matched_df[matched_df.is_match].empty:
        fp_mask = matched_df[matched_df.is_match].significance.apply(
            lambda s: not any(json.loads(s or "{}").get(bg,{}).get("significant", False) for bg in backgrounds)
        )
        fp_cons = matched_df[matched_df.is_match & fp_mask].motif_consensus.nunique()
    else:
        fp_cons = 0

    # --- Write Markdown ---
    with open(report_path,"w") as md:
        md.write(f"# MotiFab Motif Discovery Report\n\n")
        md.write(f"**Generated:** {datetime.now():%Y-%m-%d %H:%M}\n")
        md.write(f"**Reliability threshold:** {int(threshold*100)}%\n")
        md.write(f"**High-confidence threshold:** {int(HIGH_CONF*100)}%\n\n")

        # Executive Summary
        md.write("## Executive Summary\n\n")
        if not det_table.empty and det_table.detection_rate.max() > 0:
            md.write(f"- Best detection tool: {det_table.tool.iloc[det_table.detection_rate.idxmax()]} ({det_table.detection_rate.max():.0%}).\n")
        else:
            md.write("- Best detection tool: No successful detections found.\n")
            
        if not det_table.empty and det_table.significant_rate.max() > 0:
            md.write(f"- Best significance tool: {det_table.tool.iloc[det_table.significant_rate.idxmax()]} ({det_table.significant_rate.max():.0%}).\n")
        else:
            md.write("- Best significance tool: No significant matches found.\n")
        md.write("---\n\n")

        sections = [
            ("1. Detection & Significance by Tool", det_table.to_markdown(index=False, floatfmt=".2%") if not det_table.empty else "No data available"),
            ("2. Min Dataset Size by Injection Rate", min_len.to_markdown(index=False) if not min_len.empty else "No reliable detection thresholds found"),
            ("3. Min Injection Rate by Dataset Size", min_rate.to_markdown(index=False) if not min_rate.empty else "No reliable detection thresholds found"),
            ("4. Tool × Background Ranking", rank_df.to_markdown(index=False, floatfmt=".2%") if not rank_df.empty else "No ranking data available"),
            ("5. Insignificant Match Patterns", fp.to_markdown(index=False, floatfmt=".2%") if not fp.empty else "No insignificant match data available"),
            ("6. Detection vs Injection Rate", trend_ir.to_markdown(index=False, floatfmt=".2%") if not trend_ir.empty else "No injection rate trend data available"),
            ("7. Detection vs Dataset Length", trend_dl.to_markdown(index=False, floatfmt=".2%") if not trend_dl.empty else "No dataset length trend data available"),
            ("8. Motif Diversity", pd.DataFrame([{"metric":"total consensus","count":total_cons},{"metric":"matched consensus","count":matched_cons},{"metric":"insignificant consensus","count":fp_cons}]).to_markdown(index=False))
        ]
        for title, table in sections:
            md.write(f"## {title}\n\n")
            md.write(table + "\n\n")
            
            # Add explanations for each section
            if "Detection & Significance by Tool" in title:
                md.write("**What this shows:** Detection rate indicates how often each tool found any motif matching the injected motif across all experimental conditions. Significance rate shows how often the tool found a matching motif that was also statistically significant in at least one background type.\n\n")
                if not det_table.empty:
                    best_det = det_table.loc[det_table.detection_rate.idxmax()]
                    best_sig = det_table.loc[det_table.significant_rate.idxmax()]
                    md.write(f"**Key findings:** {best_det.tool} has the highest detection rate ({best_det.detection_rate:.1%}), while {best_sig.tool} has the highest significance rate ({best_sig.significant_rate:.1%}). ")
                    # Check if any tool has notably low performance
                    worst_det = det_table.loc[det_table.detection_rate.idxmin()]
                    if worst_det.detection_rate < 0.5:
                        md.write(f"{worst_det.tool} shows poor performance with only {worst_det.detection_rate:.1%} detection rate.")
                    md.write("\n\n")
                else:
                    md.write("**Key findings:** No successful detections found across any tools.\n\n")
                    
            elif "Min Dataset Size by Injection Rate" in title:
                md.write("**What this shows:** For each injection rate, this shows the minimum number of sequences needed to achieve reliable motif discovery (success in ≥70% of replicates). Lower numbers indicate better sensitivity.\n\n")
                if not min_len.empty:
                    best_rate = min_len.loc[min_len.min_dataset_length.idxmin()]
                    md.write(f"**Key findings:** The most sensitive condition is injection rate {best_rate.injection_rate} requiring only {int(best_rate.min_dataset_length)} sequences. ")
                    if len(min_len) > 1:
                        worst_rate = min_len.loc[min_len.min_dataset_length.idxmax()]
                        md.write(f"The most challenging condition is injection rate {worst_rate.injection_rate} requiring {int(worst_rate.min_dataset_length)} sequences.")
                    md.write("\n\n")
                else:
                    md.write("**Key findings:** No injection rates achieved reliable performance at the tested dataset sizes.\n\n")
                    
            elif "Min Injection Rate by Dataset Size" in title:
                md.write("**What this shows:** For each dataset size, this shows the minimum injection rate needed to achieve reliable motif discovery. Lower rates indicate better sensitivity to weak signals.\n\n")
                if not min_rate.empty:
                    best_size = min_rate.loc[min_rate.min_injection_rate.idxmin()]
                    md.write(f"**Key findings:** The most sensitive condition is {int(best_size.dataset_length)} sequences requiring only {best_size.min_injection_rate:.0%} injection rate. ")
                    if len(min_rate) > 1:
                        worst_size = min_rate.loc[min_rate.min_injection_rate.idxmax()]
                        md.write(f"The least sensitive condition is {int(worst_size.dataset_length)} sequences requiring {worst_size.min_injection_rate:.0%} injection rate.")
                    md.write("\n\n")
                else:
                    md.write("**Key findings:** No dataset sizes achieved reliable performance at the tested injection rates.\n\n")
                    
            elif "Tool × Background Ranking" in title:
                md.write("**What this shows:** Success rate for finding significant matches to the injected motif, broken down by tool and background type. This reveals which tool-background combinations work best together.\n\n")
                if not rank_df.empty and rank_df.success_rate.max() > 0:
                    top_3 = rank_df.nlargest(3, 'success_rate')
                    md.write("**Key findings:** Top performing combinations are:\n")
                    for i, (_, row) in enumerate(top_3.iterrows(), 1):
                        if row.success_rate > 0:
                            md.write(f"{i}. {row.tool} + {row.background} background ({row.success_rate:.1%})\n")
                    
                    # Find worst performing tools/backgrounds
                    tool_avg = rank_df.groupby('tool')['success_rate'].mean().sort_values(ascending=False)
                    bg_avg = rank_df.groupby('background')['success_rate'].mean().sort_values(ascending=False)
                    md.write(f"\nBest overall tool: {tool_avg.index[0]} ({tool_avg.iloc[0]:.1%} average). ")
                    md.write(f"Best overall background: {bg_avg.index[0]} ({bg_avg.iloc[0]:.1%} average).\n\n")
                else:
                    md.write("**Key findings:** No successful tool-background combinations found.\n\n")
                    
            elif "Insignificant Match Patterns" in title:
                md.write("**What this shows:** The rate at which each tool finds motifs that match the injected motif but fail statistical significance tests. High rates may indicate the tool finds many weak or false matches.\n\n")
                if not fp.empty:
                    highest_fp = fp.loc[fp.insignificant_rate.idxmax()]
                    lowest_fp = fp.loc[fp.insignificant_rate.idxmin()]
                    md.write(f"**Key findings:** {highest_fp.tool} has the highest insignificant match rate ({highest_fp.insignificant_rate:.1%}), suggesting it may be less stringent. ")
                    md.write(f"{lowest_fp.tool} has the lowest rate ({lowest_fp.insignificant_rate:.1%}), indicating more precise matching.\n\n")
                else:
                    md.write("**Key findings:** No insignificant match data available.\n\n")
                    
            elif "Detection vs Injection Rate" in title:
                md.write("**What this shows:** How detection success varies with injection rate across all tools and conditions. Higher injection rates should generally show better detection.\n\n")
                if not trend_ir.empty:
                    best_ir = trend_ir.loc[trend_ir.detection_rate.idxmax()]
                    worst_ir = trend_ir.loc[trend_ir.detection_rate.idxmin()]
                    md.write(f"**Key findings:** Best performance at injection rate {best_ir.injection_rate} ({best_ir.detection_rate:.1%}). ")
                    md.write(f"Worst performance at injection rate {worst_ir.injection_rate} ({worst_ir.detection_rate:.1%}). ")
                    if trend_ir.iloc[-1].detection_rate > trend_ir.iloc[0].detection_rate:
                        md.write("Shows expected trend of better detection at higher injection rates.\n\n")
                    else:
                        md.write("Shows unexpected trend - may indicate experimental issues.\n\n")
                else:
                    md.write("**Key findings:** No injection rate trend data available.\n\n")
                    
            elif "Detection vs Dataset Length" in title:
                md.write("**What this shows:** How detection success varies with dataset size across all tools and conditions. Larger datasets should generally show better detection.\n\n")
                if not trend_dl.empty:
                    # Extract numeric detection rates for comparison
                    trend_dl_numeric = trend_dl.copy()
                    trend_dl_numeric['detection_rate_num'] = trend_dl_numeric['detection_rate'].str.rstrip('%').astype(float) / 100
                    best_dl = trend_dl_numeric.loc[trend_dl_numeric.detection_rate_num.idxmax()]
                    worst_dl = trend_dl_numeric.loc[trend_dl_numeric.detection_rate_num.idxmin()]
                    md.write(f"**Key findings:** Best performance with {int(best_dl.dataset_length)} sequences ({best_dl.detection_rate}). ")
                    md.write(f"Worst performance with {int(worst_dl.dataset_length)} sequences ({worst_dl.detection_rate}). ")
                    if trend_dl_numeric.iloc[-1].detection_rate_num > trend_dl_numeric.iloc[0].detection_rate_num:
                        md.write("Shows expected trend of better detection with larger datasets.\n\n")
                    else:
                        md.write("Shows unexpected trend - may indicate saturation or experimental issues.\n\n")
                else:
                    md.write("**Key findings:** No dataset length trend data available.\n\n")
                    
            elif "Motif Diversity" in title:
                md.write("**What this shows:** Diversity of motifs discovered across all experiments. 'Total consensus' includes all unique motifs found, 'matched consensus' are those similar to the injected motif, and 'insignificant consensus' are matched motifs that failed significance tests.\n\n")
                match_rate = (matched_cons / total_cons * 100) if total_cons > 0 else 0
                fp_rate = (fp_cons / matched_cons * 100) if matched_cons > 0 else 0
                md.write(f"**Key findings:** Of {total_cons} unique motifs discovered, {matched_cons} ({match_rate:.1f}%) matched the injected motif. ")
                if fp_cons > 0:
                    md.write(f"Of these matches, {fp_cons} ({fp_rate:.1f}%) were insignificant, suggesting {100-fp_rate:.1f}% of matches are statistically robust.")
                else:
                    md.write("All matched motifs were statistically significant.")
                md.write("\n\n")
                
            md.write("---\n\n")

        # Heatmaps
        md.write("## Heatmaps\n\n")
        for info in heatmaps.values():
            label = "Significant Matches" if info.get("only_significant") else "All Matches"
            rel = os.path.relpath(info["path"], os.path.dirname(report_path))
            md.write(f"### {label}" + "\n\n")
            md.write(f"![{label}]({rel})" + "\n\n")

        md.write("*End of Report*\n")
    print(f"Report written to {report_path}")

# if __name__ == '__main__':
#     config_path = "/polio/oded/MotiFabEnv/test_run_FOXD1/motifab_config.json"
#     dm = DatasetManager(config_path)
#     generate_report(dm)

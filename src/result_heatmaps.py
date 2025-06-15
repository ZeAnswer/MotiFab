#!/usr/bin/env python3
"""
Generate grid heatmaps of motif discovery across dataset lengths, injection rates,
for each tool (rows) and background type (columns).
"""
import os
import json
import pandas as pd
import numpy as np
from dataset_manager import DatasetManager
import seaborn as sns
import matplotlib.pyplot as plt


def plot_discovery_heatmaps(csv_path: str,
                            output_dir: str,
                            only_significant: bool = False,
                            save_heatmap: bool = True):
    """
    Create a grid of heatmaps: rows = tools, columns = backgrounds.
    Each heatmap shows count of replicates that found the injected motif
    (is_match=True), optionally requiring significance (significant=True)
    for that background.
    Parameters:
        csv_path: path to results CSV with fields [dataset_length, injection_rate, replicate, tool, significance (JSON), is_match].
        output_dir: directory to save the heatmap PNG.
        only_significant: if True, count only matches with significant stat for the background.
    """
    # load results
    df = pd.read_csv(csv_path)
    # parse significance JSON column
    df['sig_dict'] = df['significance'].apply(lambda s: json.loads(s) if pd.notna(s) else {})

    # hard-coded experiment dimensions
    dataset_lengths = [80, 150, 500, 1000]
    injection_rates = [0.1, 0.3, 0.5, 0.7]
    replicates = [1, 2, 3, 4, 5]
    tools = ['BioProspector', 'MEME', 'Homer', 'GimmeMotifs']
    backgrounds = ['custom', 'gc', 'genomic']

    # prepare plot grid (no sharing to retain individual ticks)
    n_rows = len(tools)
    n_cols = len(backgrounds)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(4 * n_cols, 3 * n_rows),
                             sharex=False, sharey=False)
    # adjust spacing to equalize subplot widths and allow space for tool labels
    # increase left and right margins for whitespace
    fig.subplots_adjust(wspace=0.4, hspace=0.4, left=0.3, right=0.5)
    axes = np.atleast_2d(axes)

    # iterate tools and backgrounds
    for i, tool in enumerate(tools):
        for j, bg in enumerate(backgrounds):
            ax = axes[i, j]
            # initialize count matrix
            mat = pd.DataFrame(0,
                               index=injection_rates,
                               columns=dataset_lengths)
            # fill counts
            for dl in dataset_lengths:
                for ir in injection_rates:
                    count = 0
                    for rep in replicates:
                        sub = df[(df['dataset_length'] == dl) &
                                 (df['injection_rate'] == ir) &
                                 (df['replicate'] == rep) &
                                 (df['tool'] == tool)]
                        found = False
                        for _, row in sub.iterrows():
                            if not row.get('is_match'):
                                continue
                            if only_significant:
                                if not row['sig_dict'].get(bg, {}).get('significant'):
                                    continue
                            found = True
                            break
                        if found:
                            count += 1
                    mat.at[ir, dl] = count
            # plot heatmap, force consistent color scale
            # plot heatmap with integer colorbar ticks
            sns.heatmap(mat, ax=ax, annot=True, fmt='d', cmap='viridis',
                        vmin=0, vmax=len(replicates),
                        cbar=(j == n_cols - 1),
                        cbar_kws={'label': 'Matched Replicates',
                                  'ticks': list(range(0, len(replicates)+1))})
            # background title on top row
            if i == 0:
                ax.set_title(f"BG: {bg}", fontsize=12)
            # dataset length label under each heatmap
            ax.set_xlabel('Dataset Length', fontsize=9)
            # injection rate label on each heatmap y-axis
            ax.set_ylabel('Injection Rate', fontsize=9)
            # set tick labels at cell centers
            ax.set_xticks(np.arange(len(dataset_lengths)) + 0.5)
            ax.set_xticklabels(dataset_lengths, rotation=0)
            ax.set_yticks(np.arange(len(injection_rates)) + 0.5)
            ax.set_yticklabels(injection_rates, rotation=0)

    plt.tight_layout()
    # annotate tool names at row level in left margin
    for idx, tool in enumerate(tools):
        ax0 = axes[idx, 0]
        pos = ax0.get_position()
        y = pos.y0 + pos.height / 2
        # move tool labels further into margin
        fig.text(0.01, y, tool, va='center', rotation='vertical', fontsize=12)
    if save_heatmap:
        os.makedirs(output_dir, exist_ok=True)
        suffix = '_sig' if only_significant else ''
        out_file = os.path.join(output_dir, f"discovery_heatmaps{suffix}.png")
        fig.savefig(out_file)
        plt.close(fig)
        print(f"Saved discovery heatmaps to {out_file}")
    else:
        print("Displaying discovery heatmaps:")
        plt.show()

class HeatmapGenerator:
    """
    Generates discovery heatmaps using parameters from DatasetManager.
    """
    def __init__(self, dataset_manager: DatasetManager):
        self.dm = dataset_manager
        self.dgp = self.dm.get_dataset_generation_params()
        self.rdp = self.dm.get_denovo_params()
        self.hp = self.dm.get_heatmaps_generator_params()
        self.parsed = self.dm.get_parsed_results()

    def generate(self):
        output_dir = self.hp.get('output_dir', '')
        only_sig_list = self.hp.get('only_significant', [False])
        generated = {}
        # parameters for axes
        dataset_lengths = self.dgp.get('seq_amounts', [])
        injection_rates = self.dgp.get('injection_rates', [])
        replicates = list(range(1, self.dgp.get('n_replicates', 1) + 1))
        tools = self.rdp.get('tools', []) + ['GimmeMotifs']
        backgrounds = self.rdp.get('background_types', [])
        # iterate over only_significant options
        for only_sig in only_sig_list:
            # find CSV path for overall results
            csv_name = None
            for fname, meta in self.parsed.items():
                if not meta.get('only_matches') and meta.get('only_significant') == only_sig:
                    csv_name = fname
                    break
            if not csv_name:
                continue
            csv_path = self.parsed[csv_name]['path']
            # call plotting
            suffix = '_sig' if only_sig else ''
            plot_discovery_heatmaps(
                csv_path=csv_path,
                output_dir=output_dir,
                only_significant=only_sig,
                save_heatmap=True
            )
            out_file = os.path.join(output_dir, f"discovery_heatmaps{suffix}.png")
            generated[suffix if only_sig else 'all'] = {'path': out_file, 'only_significant': only_sig}
        # update config
        self.dm.update_generated_heatmap(generated)
        return generated

if __name__ == '__main__':
    # JSON-driven heatmap generation
    config_path = '/polio/oded/MotiFabEnv/presentation_run/dataset_config.json'
    dm = DatasetManager(config_path)
    hg = HeatmapGenerator(dm)
    generated = hg.generate()
    print(f"Generated heatmaps: {generated}")

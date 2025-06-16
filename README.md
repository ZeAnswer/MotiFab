# MotiFab (Motif Discovery & Analysis) User Guide

MotiFab is a JSON-driven pipeline for  
1. Generating a background FASTA  
2. Constructing injected-motif datasets  
3. Running de novo motif discovery (GimmeMotifs)  
4. Parsing and filtering results  
5. Plotting summary heatmaps  

All steps read their inputs from—and write key outputs back into—a single JSON config via `DatasetManager`. You can run steps individually or as one end-to-end job with `run_motifab.py`.
(currently running individual parts via CLI is not possible, ignore that part above and any that are written in the future)

---

## 1. Installing & Entry Point

Assuming you’ve installed MotiFab into your Python environment, run the full pipeline with:

```bash
python run_motifab.py path/to/dataset_config.json
```

This script will, in order:
1. Generate background FASTA (`FastaGenerator.generate()`)
2. Create injected-motif datasets (`DatasetGenerator.generate_datasets()`)
3. Launch GimmeMotifs on each replicate (`DenovoRunner.run_denovo()`)
4. Parse motif files into CSV (`ResultsParser.run_all()`)
5. Build heatmaps (`HeatmapGenerator.generate()`)

You may also invoke each step directly by running its module’s `__main__` (see individual class docs below). (ignore this, not currently implemented)

---

## 2. JSON Configuration Overview

Your config file is standard JSON. Top-level keys:

```json
{
  "fasta_generation_params":    { … },
  "dataset_generation_params": { … },
  "run_denovo_params":          { … },
  "match_params":               { … },
  "result_parser_params":       { … },
  "heatmaps_generator_params":  { … },

  /* The following are read/updated by pipeline steps: */
  "combinations":        { /* dataset layouts */ },
  "parsed_results":      { /* CSV metadata */ },
  "generated_heatmap":   { /* heatmap metadata */ }
}
```
### 2.1 fasta_generation_params
Generates a “master” background FASTA.

| Key         | Type             | Default | Required | Notes                                                       |
|-------------|------------------|---------|----------|-------------------------------------------------------------|
| `outfile`   | string (filepath)|         | ✔︎        | Path to write the background FASTA.                         |
| `bg_type`   | string           |         | ✔︎        | One of: `random`, `genomic`, `gc`, `promoter`, `true_random`. |
| `fmt`       | string           |         | ✔︎        | Format: `fasta` or `bed`.                                   |
| `size`      | int              |         | ✔︎        | Size of sequences                                 |
| `number`    | int              |         |          | Amount of sequences to generate.                                           |
| `inputfile` | string (filepath)|         | conditional | Path to input FASTA; required for `random` and `gc` background types. |
| `genome`    | string (filepath)|         | conditional | Required for `genomic`, `gc`, `promoter`.                  |
| `gc_content`| float            |         | conditional | Required for `true_random`; also used for `gc` backgrounds when no `inputfile` is provided. |

**Pipeline effect**: writes `outfile`, updates  
```json
dataset_generation_params.master_fasta = "<outfile>"
```

---

### 2.2 dataset_generation_params
Controls injected-motif dataset creation.

| Key                | Type               | Default | Required     | Notes                                                   |
|--------------------|--------------------|---------|--------------|---------------------------------------------------------|
| `pfm`              | string (filepath)  |         | ✔︎ one of `{pfm, ppm, consensus}` | Path to a Position Frequency Matrix file.              |
| `ppm`              | string (filepath)  |         | ✔︎            | Path to a Position Probability Matrix file.            |
| `consensus`        | string             |         | ✔︎            | IUPAC consensus sequence; built into a PPM.            |
| `mutation_rate`    | float              | 0.0     |              | Chances for a base in a 'simple' `consensus` (made out of only ACTG letters) to randomly mutate |
| `output_dir`       | string (dir)       |         | ✔︎            | Root for all generated datasets.                        |
| `seq_amounts`      | [int,…]            |         | ✔︎            | Sequence counts per dataset (e.g. `[80,150]`).         |
| `injection_rates`  | [float,…]          |         | ✔︎            | Fraction injected, `0.0–1.0` (e.g. `[0.3,0.45]`).       |
| `n_replicates`     | int                |         | ✔︎            | Number of independent replicates per config.           |
| `background_length`| int                |         | ✔︎            | Length of background regions for injection.            |
| *(optional)* `force` | bool             | false   |              | Force regeneration of all combos (default skips existing)|

**Validation**: Exactly one of `pfm`, `ppm` or `consensus` must be set.  
**Pipeline effect**: after running, `"combinations"` is filled with dataset/replicate entries.

---


### 2.3 run_denovo_params
Configures GimmeMotifs de novo discovery.

| Key               | Type       | Default | Required | Notes                                                          |
|-------------------|------------|---------|----------|----------------------------------------------------------------|
| `background_types`| [string,…] |         | ✔︎        | Any of `{random, genomic, gc, custom}`                         |
| `genome_fasta`    | string     |         | ✔︎ for genomic/gc | Path to reference genome for sampling.                    |
| `tools`           | [string,…] |         | ✔︎        | Motif tools: e.g. `["BioProspector","MEME","Homer"]`          |
| `ncpus`           | int        |         | ✔︎        | Number of CPUs per replicate.                                |
| `max_parallel`    | int        |         | ✔︎        | Concurrent replicates.                                        |
| `rerun_failed`    | bool       | false   |          | Also re-run any replicate with `status="failed_denovo"`.     |
| `force`           | bool       | false   |          | Force re­run all replicates regardless of previous status.   |

**Pipeline effect**: updates each replicate’s  
```json
replicates[].status, gimme_out_dir,
gimme_denovo, gimme_stats, all_motifs, all_motifs_stats
```

---

### 2.4 match_params
Controls matching of discovered vs injected motif.

| Key       | Type   | Default    | Notes                                                |
|-----------|--------|------------|------------------------------------------------------|
| `match`   | string | `"partial"`| One of `{partial, subtotal, total}`.                  |
| `metric`  | string | `"seqcor"` | One of `{seqcor,pcc,ed,distance,wic,chisq,akl,ssd}`. |
| `combine` | string | `"mean"`   | One of `{mean,sum}`.                                 |
| `min_score`| float | `0.7`      | Score threshold to call `is_match=True`.             |

---

### 2.5 result_parser_params
Determines which CSV exports to create.

```json
"result_parser_params": {
  "dumps": [
    {
      "filename":         "all_discovered_motifs.csv",
      "only_matches":     false,
      "only_significant": false
    },
    {
      "filename":         "matched_discovered_motifs.csv",
      "only_matches":     true,
      "only_significant": false
    },
    {
      "filename":         "significant_discovered_motifs.csv",
      "only_matches":     false,
      "only_significant": true
    }
  ]
}
```

- `only_matches`: include only motifs with `is_match=True`.  
- `only_significant`: include only motifs whose stats contain ≥ 1 `significant=true`.  

**Pipeline effect**: writes `"parsed_results": { filename → {path, only_matches, only_significant} }`.

---

### 2.6 heatmaps_generator_params
Controls heatmap output.

| Key                | Type         | Default | Notes                                                                                   |
|--------------------|--------------|---------|-----------------------------------------------------------------------------------------|
| `output_dir`       | string (dir) |         | Directory to save PNGs.                                                                 |

**Pipeline effect**: writes  
```json
"generated_heatmap": {
  "all": {path, only_significant:false},
  "sig": {path, only_significant:true}
}
```

---
(ignore this)
<!-- ## 3. Step-by-Step Usage

1. **Create your JSON**  
   Fill all required keys (see above).  
2. **Generate background**  
   ```bash
   python -m fasta_generator --config config.json
   ```
   or via `run_motifab.py`.  
3. **Build datasets**  
   ```bash
   python -m dataset_generator --config config.json
   ```
4. **Run de novo**  
   ```bash
   python -m denovo_runner --config config.json
   ```
5. **Parse into CSV**  
   ```bash
   python -m results_parser --config config.json
   ```
6. **Plot heatmaps**  
   ```bash
   python -m result_heatmaps --config config.json
   ```

Or simply:

```bash
python run_motifab.py --config config.json
``` -->

---

## 4. Notes & Tips

- **Mutually exclusive inputs**: In `dataset_generation_params`, exactly one of `pfm`, `ppm`, or `consensus` must be provided.  
- **Re-running**:  
  - `force=true` in any section forces that entire step to overwrite existing outputs.  
  - `rerun_failed=true` in `run_denovo_params` re-runs only failed replicates.  
- **Output structure**: All generated files are placed under `output_dir`, with subdirectories for each combination and replicate.  
- **Customizing**: You may adjust any parameter in the JSON, re-run individual steps, and outputs will be picked up (or skipped) based on your flags.  

---

This completes the full MotiFab usage documentation. Let me know if you’d like examples or further detail on any step!
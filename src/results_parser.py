import os
import json
import csv
from dataset_manager import DatasetManager
from gimmemotifs_plus.motif_plus import MotifPlus
from gimmemotifs.comparison import MotifComparer
class ResultsParser:
    def __init__(self, dataset_manager: DatasetManager):
        self.dataset_manager = dataset_manager
        # load parser params from config
        self.parser_params = dataset_manager.get_result_parser_params().get('dumps', [])
        self.injected_motif = dataset_manager.get_motifp()
        # results structure: replicate_name -> motif_id -> data dict
        self.results: dict[str, dict[str, dict]] = {}
        # preload all replicate metadata
        self._all_reps = dataset_manager.get_all_reps()
        
    def _get_tool_name_from_motif_id(self, motif_id: str) -> str:
        """Extract the tool name from the motif ID.
        For Gimme motifs, the format is '>GimmeMotifs_{i}'
        For any other motif, the format is '>gimme_{i}_{tool_name}_w{j}_{k}'
        Args:
            motif_id (str): The motif ID string."""
        if motif_id.startswith('GimmeMotifs_'):
            return 'GimmeMotifs'
        elif motif_id.startswith('gimme_'):
            parts = motif_id.split('_')
            if len(parts) >= 3:
                return parts[2]
        return 'unknown'

    def load_motifs(self):
        """
        Load all discovered motifs for each replicate into self.results.
        Uses gimmeMotifs read_motifs(as_dict=True).
        """
        import os
        from gimmemotifs.motif.read import read_motifs
        # iterate replicates
        for rep in self._all_reps:
            name = rep['name']
            self.results[name] = {}
            # load primary denovo motifs
            gimme_fp = rep.get('gimme_denovo')
            if gimme_fp:
                if not os.path.exists(gimme_fp):
                    print(f"Warning: de-novo motif file not found for {name}: {gimme_fp}")
                else:
                    try:
                        motifs = read_motifs(gimme_fp, as_dict=True)
                    except Exception as e:
                        print(f"Warning: failed to read de-novo motifs for {name}: {e}")
                    else:
                        for mid, mobj in motifs.items():
                            self.results[name][mid] = {
                                'motif': mobj,
                                'stats': {},
                                'image_path': None,
                                'tool': self._get_tool_name_from_motif_id(mid),
                                'match': {}
                            }
            # load combined motifs
            all_fp = rep.get('all_motifs')
            if all_fp:
                if not os.path.exists(all_fp):
                    print(f"Warning: combined motifs file not found for {name}: {all_fp}")
                else:
                    try:
                        extra = read_motifs(all_fp, as_dict=True)
                    except Exception as e:
                        print(f"Warning: failed to read combined motifs for {name}: {e}")
                    else:
                        for mid, mobj in extra.items():
                            if mid not in self.results[name]:
                                self.results[name][mid] = {
                                    'motif': mobj,
                                    'stats': {},
                                    # 'image_path': None,
                                    'tool': self._get_tool_name_from_motif_id(mid),
                                    'match': {}
                                }
        return self.results

    def populate_stats(self):
        """
        Parse stats files and populate self.results[rep][motif]['stats'][background] = stats_dict.
        """
        import os
        # helper to parse only p-values from stats file
        def _parse_pvalues(path):
            pmap = {}
            if not path or not os.path.exists(path):
                return pmap
            with open(path) as fh:
                header = None
                idx = None
                for line in fh:
                    if line.startswith('#'):
                        continue
                    parts = line.strip().split('\t')
                    # header row
                    if header is None:
                        header = parts
                        try:
                            idx = header.index('phyper_at_fpr')
                        except ValueError:
                            # no p-value column
                            return pmap
                        continue
                    # data rows
                    sid = parts[0]
                    try:
                        p = float(parts[idx])
                    except (IndexError, ValueError):
                        continue
                    pmap[sid] = p
            return pmap

        # iterate each replicate and populate stats
        threshold = 0.05
        for rep in self._all_reps:
            name = rep['name']
            entry = self.results.get(name, {})
            # gimme_stats: phyper p-values
            for bg, path in rep.get('gimme_stats', {}).items():
                pmap = _parse_pvalues(path)
                for mid, data in entry.items():
                    if mid in pmap:
                        pval = pmap[mid]
                        data['stats'][bg] = {
                            'p_value': pval,
                            'significant': (pval < threshold)
                        }
            # all_motifs_stats: same p-value
            for bg, path in rep.get('all_motifs_stats', {}).items():
                pmap = _parse_pvalues(path)
                for mid, data in entry.items():
                    if mid in pmap:
                        pval = pmap[mid]
                        data['stats'][bg] = {
                            'p_value': pval,
                            'significant': (pval < threshold)
                        }
        return self.results

    # def populate_images(self):
    #     """
    #     Assign a placeholder image path for each motif; actual images dir is rep['gimme_out_dir']/images
    #     """
    #     import os
    #     for rep in self._all_reps:
    #         name = rep['name']
    #         images_dir = os.path.join(rep.get('gimme_out_dir', ''), 'images')
    #         # TODO: use real image files e.g. os.path.join(images_dir, f"{mid}.png")
    #         for mid, data in self.results.get(name, {}).items():
    #             data['image_path'] = images_dir
    #     return self.results
    def populate_matches(self):
        """
        Compare each found motif against the injected motif and mark score & match flag.
        Uses MotifComparer.get_all_scores with parameters from dataset_manager.get_match_params().

        match possible values: 'partial', 'subtotal', 'total'. Default is 'partial'. Not all metrics use this.
        combine possible values: 'mean', 'sum'. Default is 'mean'. Not all metrics use this.
        metric possible values:
          - 'seqcor': Pearson correlation of motif scores along sequence.
          - 'pcc': Pearson correlation coefficient of motif PFMs.
          - 'ed': Euclidean distance-based similarity of motif PFMs.
          - 'distance': Distance-based similarity of motif PFMs.
          - 'wic': Weighted Information Content, see van Heeringen 2011.
          - 'chisq': Chi-squared similarity of motif PFMs.
          - 'akl': Similarity based on average Kullback-Leibler divergence, see Mahony 2011.
          - 'ssd': Sum of squared distances of motif PFMs.
        Default metric is 'seqcor'.
        """
        # load match settings
        params = self.dataset_manager.get_match_params()
        match = params.get('match', 'partial')
        metric = params.get('metric', 'seqcor')
        combine = params.get('combine', 'mean')
        min_score = params.get('min_score', 0.7)
        # validate and init comparer
        allowed_matches = {'partial', 'subtotal', 'total'}
        allowed_combine = {'mean', 'sum'}
        allowed_metrics = {'seqcor', 'pcc', 'ed', 'distance', 'wic', 'chisq', 'akl', 'ssd'}
        if match not in allowed_matches:
            raise ValueError(f"Invalid 'match' parameter: {match}. Allowed values are: {allowed_matches}")
        if combine not in allowed_combine:
            raise ValueError(f"Invalid 'combine' parameter: {combine}. Allowed values are: {allowed_combine}")
        if metric not in allowed_metrics:
            raise ValueError(f"Invalid 'metric' parameter: {metric}. Allowed values are: {allowed_metrics}")
        # init comparer
        mc = MotifComparer()
        # for each replicate
        for rep in self._all_reps:
            name = rep['name']
            entry = self.results.get(name, {})
            # collect motif objects and ids
            mids = list(entry.keys())
            motifs = [entry[mid]['motif'] for mid in mids]
            # compare to injected
            scores = mc.get_all_scores(
                motifs=motifs,
                dbmotifs=[self.injected_motif],
                match=match,
                metric=metric,
                combine=combine,
                pval=False
            ) or {}
            # assign match info per motif
            injected_id = self.injected_motif.id
            for mid in mids:
                data = entry[mid]
                info = scores.get(mid, {}).get(injected_id)
                if info and isinstance(info, (list, tuple)):
                    score_val = info[0]
                    data['match'] = {
                        'score': score_val,
                        'is_match': (score_val >= min_score)
                    }
                else:
                    data['match'] = {
                        'score': None,
                        'is_match': False
                    }
        return self.results
    
    def dump_to_csv(self, filename: str, only_matches: bool = False, only_significant: bool = False):
        """
        Dump the parsed results into a CSV file.
        filename: name of the CSV file (e.g., 'all_discovered_motifs.csv' or 'matched_discovered_motifs.csv').
        only_matches: if True, include only motifs with is_match=True.
        only_significant: if True, include only motifs with at least one significant stat and filter stats to only those.
        The output directory is taken from dataset_manager.get_output_dir().
        """
        import os, csv, json
        # ensure results are populated
        if not self.results:
            raise ValueError("No results to dump. Run load_motifs, populate_stats, and populate_matches first.")
        outdir = self.dataset_manager.get_output_dir()
        os.makedirs(outdir, exist_ok=True)
        path = os.path.join(outdir, filename)
        headers = [
            'dataset_length', 'injection_rate', 'replicate',
            'tool', 'motif_id', 'motif_consensus',
            'significance', 'match_score', 'is_match'
        ]
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            # iterate in grouped order: combination -> replicate -> tool -> motif_id
            for rep_name in sorted(self.results.keys()):
                # parse replicate name: 'len_{len}_rate_{rate}_rep_{rep}'
                base, _, rep_num = rep_name.rpartition('_rep_')
                parts = base.split('_')  # ['len','20','rate','30']
                try:
                    dataset_length = int(parts[1])
                    injection_rate = int(parts[3]) / 100.0
                except (IndexError, ValueError):
                    dataset_length = None
                    injection_rate = None
                rep_data = self.results[rep_name]
                # sort by tool then motif_id
                for motif_id, data in sorted(rep_data.items(), key=lambda x: (x[1]['tool'], x[0])):
                    # skip non-matches if requested
                    if only_matches and not data.get('match', {}).get('is_match', False):
                        continue
                    # filter stats by significance if requested
                    orig_stats = data.get('stats', {})
                    if only_significant:
                        filtered_stats = {bg: st for bg, st in orig_stats.items() if st.get('significant')}
                        if not filtered_stats:
                            continue
                    else:
                        filtered_stats = orig_stats
                    consensus = data['motif'].to_consensus()
                    significance = json.dumps(filtered_stats)
                    score = data.get('match', {}).get('score')
                    is_match = data.get('match', {}).get('is_match')
                    writer.writerow([
                        dataset_length, injection_rate, rep_num,
                        data['tool'], motif_id, consensus,
                        significance, score, is_match
                    ])
        return path
    
    def run_all(self):
        """
        Execute full parsing workflow: load motifs, stats, matches, dump CSVs per config,
        and update parsed_results in JSON.
        Returns a dict of parsed result metadata.
        """
        # load and process
        self.load_motifs()
        self.populate_stats()
        self.populate_matches()
        # generate CSVs as per parser_params
        parsed = {}
        for entry in self.parser_params:
            fname = entry.get('filename')
            only_matches = entry.get('only_matches', False)
            only_sig = entry.get('only_significant', False)
            path = self.dump_to_csv(fname, only_matches, only_sig)
            parsed[fname] = {'path': path, 'only_matches': only_matches, 'only_significant': only_sig}
        # update config
        self.dataset_manager.update_parsed_results(parsed)
        return parsed
    
    
# if __name__ == "__main__":
#     # JSON-driven parsing workflow
#     manager = DatasetManager('/polio/oded/MotiFabEnv/test_run_FOXD1/motifab_config.json')
#     parser = ResultsParser(manager)
#     results = parser.run_all()
#     print(f"Parsed results updated: {results}")
        
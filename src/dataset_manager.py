import os
import json
from datetime import datetime
from typing import List, Optional
from gimmemotifs_plus.fasta_plus import FastaPlus
from gimmemotifs_plus.motif_plus import MotifPlus
from gimmemotifs_plus.genome_plus import GenomePlus

"""background types note:
if "genomic", "gc", or "promoter", a genome must be provided.
if "random", inputfile must be provided.
if "true_random", either inputfile or gc_content must be provided.
if "genomic" or "promoter", either inputfile or size must be provided.

for anywhere an inputfile is required, the note is relevant only for the FASTA generator. 
Can be ignored for run de novo backgrounds where inputfile is implicitly provided.
"""
bg_types = ["random", "genomic", "gc"]#, "promoter"] #TODO: for now promoter is causing a bug "ERROR - Could not find a gene file for genome...". so far I couldn't fix it. will continue later on
ext_bg_types = bg_types + ["true_random"]
available_tools = [
    "BioProspector", "MEME", "Homer",
    "AMD", "ChIPMunk", "DiNAMO", "DREME", "GADEM", "HMS",
    "Improbizer", "MDmodule", "MEMEW", "MotifSampler", "Posmo", "ProSampler",
    "RPMCMC", "Trawler", "Weeder", "XXmotif", "YAMDA"
]
matching_metrics = [
    "seqcor", "pcc", "ed", "distance", "wic", "chisq", "akl", "ssd"
]
matching_modes = [
    "partial", "subtotal", "total"
]
combine_modes = [
    "mean", "sum"
]
empty_config = {
    "master_fasta": None,
    "output_dir": None,
    "motif":{ #only one of "consensus", "pfm", or "ppm" should be provided. if "consensus" is provided and all letters are ACTG, mutation_rate may be provided.
        "consensus": None,
        "mutation_rate": None,
        "pfm": None,
        "ppm": None
    },
    "combinations_configurations":{
        "seq_amounts": [],
        "injection_rates": [],
        "n_replicates": None
    },
    "genome_configurations":{
        "genome": None,
        "install_genome": False #if true, will install the genome if not found
    },
    "fasta_generation_params": {
        "name": None,
        "bg_type": None, #ext_bg_types
        "seq_length": None,
        "seq_amount": None,
        "inputfile": None,
        "gc_content": None
    },
    "dataset_generation_params": {
        "background_length": None,
        "force": False
    },
    "run_denovo_params": {
        "background_types": None, #[bg_types],
        "ncpus": None,
        "tools": None, #[available_tools]
        "max_parallel": None,
        "rerun_failed": False, # is this even implemented?
        "force": False, #if true, will overwrite existing files
    },
    "match_params": {
        "match": None, #matching_modes
        "metric": None, #matching_metrics
        "combine": None, #combine_modes
        "min_score": None
    }
}

class DatasetManager:
   
   
    def get_parsed_results(self) -> dict:
        """Retrieve parsed results file paths from the config."""
        return self.config.get('parsed_results', {})
    def update_parsed_results(self, results: dict) -> None:
        """Update parsed results file paths in the config."""
        self.config['parsed_results'] = results
        self._update_config()
    def get_generated_heatmap(self) -> dict:
        """Retrieve generated heatmap file paths from the config."""
        return self.config.get('generated_heatmap', {})
    def update_generated_heatmap(self, results: dict) -> None:
        """Update generated heatmap file paths in the config."""
        self.config['generated_heatmap'] = results
        self._update_config()

    #@@@@ COMBO METHODS @@@@@@@@ COMBO METHODS @@@@@@@@ COMBO METHODS @@@@@@@@ COMBO METHODS @@@@@@@@
    #TODO DEFINITELY gonna have to refactor this later, it's a mess. maybe subclass? for later anyway
    def get_combo_name(self, seq_amount: int, injection_rate: float) -> str:
        """Generate a unique name for the combination based on seq_amount and injection_rate."""
        return f"len_{seq_amount}_rate_{int(injection_rate * 100)}"
    
    def get_combo_path(self, out_dir, seq_amount: int, injection_rate: float) -> str:
        """Generate a dir path for the combination based on seq_amount and injection_rate."""
        name = self.get_combo_name(seq_amount, injection_rate)
        return os.path.join(out_dir, name)

    def get_combo(self, seq_amount: int, injection_rate: float) -> Optional[dict]:
        """Retrieve a combination entry by seq_amount and injection_rate."""
        key = self.get_combo_name(seq_amount, injection_rate)
        return self.config.get('combinations', {}).get(key)
    
    def get_combo_by_name(self, name: str) -> Optional[dict]:
        """Retrieve a combination entry by its name."""
        return self.config.get('combinations', {}).get(name)
    
    def get_combos_dict(self) -> List[dict]:
        """Retrieve all combination entries."""
        return self.config.get('combinations', {})
    
    def upsert_combo_by_name(self, name: str, combination: dict) -> None:
        """Insert or update a combination entry by its name."""
        if 'combinations' not in self.config:
            self.config['combinations'] = {}
        self.config['combinations'][name] = combination
        # Save the updated config to file
        self._update_config()
            
    def upsert_combo(self, seq_amount: int, injection_rate: float, combination: dict) -> None:
        #TODO: probably need validatoins
        """Insert or update a combination entry in the config."""
        key = self.get_combo_name(seq_amount, injection_rate)
        self.upsert_combo_by_name(key, combination)
    
    def get_rep_name(self, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a name for a replicate based on seq_amount, injection_rate, and replicate number."""
        return f"{self.get_combo_name(seq_amount, injection_rate)}_rep_{replicate}"
    
    def get_rep_dir_path(self, out_dir, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a directory path for a replicate based on seq_amount, injection_rate, and replicate number."""
        combination_path = self.get_combo_path(out_dir, seq_amount, injection_rate)
        rep_name = self.get_rep_name(seq_amount, injection_rate, replicate)
        return os.path.join(combination_path, rep_name)

    def get_rep_file_path(self, out_dir, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a file path for a replicate based on seq_amount, injection_rate, and replicate number."""
        replicate_dir = self.get_rep_dir_path(out_dir, seq_amount, injection_rate, replicate)
        return os.path.join(replicate_dir, f"{self.get_rep_name(seq_amount, injection_rate, replicate)}.fa")
    
    def get_rep_by_name(self, name: str) -> Optional[dict]:
        combo_name, _, rep_num = name.rpartition('_rep_')
        combo = self.get_combo_by_name(combo_name)
        if combo and 'replicates' in combo:
            return combo['replicates'].get(name)
        else:
            # If the name does not match the expected format, return None
            print(f"Replicate {name} not found in combinations.")
        return None
    
    def get_rep(self, seq_amount: int, injection_rate: float, replicate: int) -> Optional[dict]:
        """Retrieve a replicate entry by seq_amount, injection_rate, and replicate number."""
        combination = self.get_combo(seq_amount, injection_rate)
        rep_name = self.get_rep_name(seq_amount, injection_rate, replicate)
        if combination:
            return combination.get('replicates', {}).get(rep_name)
        return None
    
    def get_all_reps(self) -> List[dict]:
        """Retrieve all replicate entries across all combinations."""
        all_replicates = []
        for combo in self.config.get('combinations', {}).values():
            if 'replicates' in combo:
                all_replicates.extend(combo['replicates'].values())
        return all_replicates
    
    def upsert_rep_by_name(self, name: str, replicate_data: dict) -> None:
        """Insert or update a replicate entry by its name."""
        combo_name, _, rep_num = name.rpartition('_rep_')
        combination = self.get_combo_by_name(combo_name)
        if not combination:
            raise ValueError(f"No combination found for name={combo_name}")
        
        if 'replicates' not in combination:
            combination['replicates'] = {}
        
        combination['replicates'][name] = replicate_data
        # Save the updated config to file
        self.upsert_combo_by_name(combo_name, combination)
        #TODO: this might cause a race condition if multiple threads try to update the same combination
        #TODO: also need validations
    
    def upsert_rep(self, seq_amount: int, injection_rate: float, replicate: int, replicate_data: dict) -> None:
        """Insert or update a replicate entry in the combination."""
        combination = self.get_combo(seq_amount, injection_rate)
        if not combination:
            raise ValueError(f"No combination found for seq_amount={seq_amount}, injection_rate={injection_rate}")
        
        if 'replicates' not in combination:
            combination['replicates'] = {}
        
        rep_name = self.get_rep_name(seq_amount, injection_rate, replicate)
        combination['replicates'][rep_name] = replicate_data
        #TODO: this might cause a race condition if multiple threads try to update the same combination
        #TODO: also need validations
        # Save the updated config to file
        self.upsert_combo(seq_amount, injection_rate, combination)
 
    #@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ REDO @@@@@@@@ 

    def __init__(self, config_path: str):
        """Initialize DatasetManager with JSON config file.
        if the file does not exist, it will be created as an empty json file."""
        self.config_path = config_path
        if not os.path.exists(config_path): #TODO: actually maybe we'd want to raise an error here if the file does not exist, force people to provide a config file
            # initialize config with nested generation params
            self.config = empty_config.copy()
            with open(config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
        else:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
                self._init_properties()  # Initialize properties from config
                self._init_genome()
    
    def _init_properties(self):
        """Initialize properties for the DatasetManager."""
        self.motif = self._get_motif()
        self.motifp = self._get_motifp()  # Initialize motifp from config if available
        self.output_dir = self._get_output_dir()  # Initialize output_dir from config if available
        self.combinations_cnfg = self._get_combinations_cnfg()  # Initialize combinations configurations from config if available
        self.genome = self._get_genome_dict().get('genome', None)  # Initialize genome from config if available
    
    def _init_genome(self):
        genome_cfg = self.config.get('genome_configurations', {})
        genome = genome_cfg.get('genome', None)
        genomep = GenomePlus(genome=genome)
        try:
            genomep.resolve(genome_cfg.get('install_genome', False))  # Resolve the genome path, installing if necessary
        except Exception as e:
            raise ValueError(f"Failed to resolve genome: {e}")

    def _update_config(self):
        """Update the config file with the current state of the DatasetManager."""
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=4)
        self._init_properties()
        
        
    # @@@@@@ Getters and Setters @@@@@@
    
    def _get_motifp(self) -> Optional[MotifPlus]:
        """Retrieve the injected motif as a MotifPlus object."""
        motifdict = self.motif
        # Check that at least one of pfm, ppm, or consensus is provided
        if not any(key in motifdict for key in ['pfm', 'ppm', 'consensus']):
            raise ValueError("Exactly one of pfm, ppm, or consensus must be provided in dataset generation parameters")
        
        motifp = MotifPlus(
            pfm=motifdict.get('pfm'),
            ppm=motifdict.get('ppm'),
            consensus=motifdict.get('consensus'),
            mutation_rate=motifdict.get('mutation_rate', 0.0)
        )
        if motifp:
            return motifp
        else:
            raise ValueError("No valid motif data found in dataset generation parameters")

    def get_motifp(self) -> Optional[MotifPlus]:
        """Retrieve the injected motif as a MotifPlus object."""
        if not self.motifp:
            self.motifp = self._get_motifp()
        return self.motifp

    def _get_output_dir(self) -> str:
        """Retrieve the output directory from the dataset generation parameters. if not provided, use current directory and create a directory within it with an appropriate name."""
        output_dir = self.config.get('output_dir', '')
        if not output_dir:
            # Use current directory and create a subdirectory with current timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join(os.getcwd(), f"MotiFab_output_{timestamp}") #TODO potential bug if activating multiple runs at the same second
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        # Return the output directory path
        self.config['output_dir'] = output_dir  # Update config with the output directory
        return output_dir
    
    def get_output_dir(self) -> str:
        """Retrieve the output directory from the config. If not set, initialize it."""
        if not self.output_dir:
            self.output_dir = self._get_output_dir()
        return self.output_dir
    
    def _get_motif(self):
        return self.config.get('motif', None)
    
    def set_master_fasta(self, fasta_path: str) -> None:
        """Set the master fasta file path in the config."""
        if not os.path.exists(fasta_path):
            raise FileNotFoundError(f"Master fasta file {fasta_path} does not exist.")
        self.config['master_fasta'] = fasta_path
        # Save the updated config to file
        self._update_config()  # Update the config file with the new master fasta path

    def get_master_fasta(self) -> Optional[str]:
        """Retrieve the master fasta file path from the config."""
        master_fasta = self.config.get('master_fasta', None)
        if master_fasta and os.path.exists(master_fasta):
            return master_fasta
        else:
            raise ValueError("Master fasta file is not set or does not exist in the config.")

    def _get_combinations_cnfg(self) -> dict:
        """Retrieve the combinations configurations from the config."""
        return self.config.get('combinations_configurations', {})
    
    def _get_genome_dict(self) -> Optional[str]:
        """Retrieve the genome from the genome configurations."""
        return self.config.get('genome_configurations', {})
    
    def _update_genome(self, genome: str) -> None:
        """Update the genome in the genome configurations."""
        if not genome:
            raise ValueError("Genome cannot be None or empty.")
        self.config['genome_configurations']['genome'] = genome
        # Save the updated config to file
        self._update_config()
    # @@@@@@@@@@@@@ flags or somth @@@@@@@@@@@@@@@
    
    def is_master_fasta_provided(self) -> bool: #TODO is master not politically correct or something like that? should I care?
        """Check if master_fasta was provided in the config."""
        return self.config.get('master_fasta') is not None and os.path.exists(self.config['master_fasta'])


    # @@@@@@@@@@ params generators @@@@@@@@
    
    def get_fasta_generation_params(self) -> dict: #TODO maybe add validations later on for the config file. not now though.
        """Generates a params dict for the fasta generator based on the config."""
        fasta_generation_params = self.config.get('fasta_generation_params', {})
        params = {
            "outfile": f"{self.output_dir}/{fasta_generation_params.get('name', 'master_fasta')}.fa",
            "bg_type": fasta_generation_params.get('bg_type', 'true_random'),
            "fmt": "fasta",
            "size": fasta_generation_params.get('seq_length', None), #TODO should I add defaults to this? or leave this to the validator
            "number": fasta_generation_params.get('seq_amount', None),
            "inputfile": fasta_generation_params.get('inputfile', None),
            "gc_content": fasta_generation_params.get('gc_content', None),
            "genome": self.genome,  #TODO remember that we need to install a genome before we reach here
        }
        return params
    
    def update_fasta_generation_params(self, params: dict) -> None:
        """Update the fasta generation parameters in the config. make sure to only take the params that are relevant to the fasta generator."""
        outfile = params.get('outfile', None)
        name = os.path.basename(outfile).replace(".fa", "")
        relevant_params = {
            "name": name,
            "bg_type": params.get('bg_type', None),
            "seq_length": params.get('size', None),
            "seq_amount": params.get('number', None),
            "inputfile": params.get('inputfile', None),
            "gc_content": params.get('gc_content', None),
        }
        # Update the fasta generation parameters in the config
        self.config['fasta_generation_params'] = relevant_params
        # Save the updated config to file
        self._update_config()

    def get_dataset_generation_params(self) -> dict:
        """Generates a params dict for the fasta generator based on the config."""
        dataset_generation_params = self.config.get('dataset_generation_params', {})
        motif = self.motif
        combination_cnfg = self.combinations_cnfg
        params = {
            "background_length": dataset_generation_params.get('background_length', 1000), #TODO should I add defaults to this? or leave this to the validator
            "force": dataset_generation_params.get('force', False), #TODO is this even implemented?
            "output_dir": f"{self.output_dir}/datasets",  # Use the output directory from the config
            "pfm": motif.get('pfm', None),  # PFM is optional
            "ppm": motif.get("ppm", None),
            "consensus": motif.get("consensus", None),
            "mutation_rate": motif.get("mutation_rate", None),
            "seq_amounts": combination_cnfg.get("seq_amounts", None),
            "injection_rates": combination_cnfg.get("injection_rates", None),
            "n_replicates": combination_cnfg.get("n_replicates", 1)  # Default to 1 if not specified
        }
        return params
        
    def update_dataset_generation_params(self, params: dict) -> None:
        """Update the dataset generation parameters in the config. make sure to only take the params that are relevant to the dataset generator."""
        relevant_params = {
            "background_length": params.get('background_length', None),  # Default to 1000 if not specified
            "force": params.get('force', False),  # Default to False if not specified
        }
        # Update the dataset generation parameters in the config
        self.config['dataset_generation_params'] = relevant_params
        # Save the updated config to file
        self._update_config()

    def get_denovo_params(self) -> dict:
        """Generates a params dict for the de novo motif discovery based on the config."""
        denovo_params = self.config.get('run_denovo_params', {})
        params = {
            "background_types": denovo_params.get('background_types', 'genomic'), 
            "ncpus": denovo_params.get('ncpus', 1),  # Default to 1 if not specified
            "tools": denovo_params.get('tools', ["BioProspector", "MEME", "Homer"]),  # Default
            "max_parallel": denovo_params.get('max_parallel', 1),  # Default to 1 if not specified
            "rerun_failed": denovo_params.get('rerun_failed', False),  # Default to False if not specified
            "genome": self.genome,  # Genome fasta path from genome configurations
            "force": denovo_params.get('force', False)  # Default to False if not specified
        }
        return params
    
    def update_denovo_params(self, params: dict) -> None:
        """Update the de novo motif discovery parameters in the config. make sure to only take the params that are relevant to the de novo motif discovery."""
        relevant_params = {
            "background_types": params.get('background_types', 'genomic'),  # Default to 'genomic' if not specified
            "ncpus": params.get('ncpus', 1),  # Default to 1 if not specified
            "tools": params.get('tools', ["BioProspector", "MEME", "Homer"]),  # Default tools
            "max_parallel": params.get('max_parallel', 1),  # Default to 1 if not specified
            "rerun_failed": params.get('rerun_failed', False),  # Default to False if not specified
            "force": params.get('force', False)  # Default to False if not specified
        }
        # Update the de novo motif discovery parameters in the config
        self.config['run_denovo_params'] = relevant_params
        # Save the updated config to file
        self._update_config()

    def get_match_params(self) -> dict:
        """Retrieve the match parameters from the config."""
        match_params = self.config.get('match_params', {})
        params = {
            "match": match_params.get('match', 'partial'),  # Default to 'partial' if not specified
            "metric": match_params.get('metric', 'seqcor'),  # Default to 'seqcor' if not specified
            "combine": match_params.get('combine', 'mean'),  # Default to 'mean' if not specified
            "min_score": match_params.get('min_score', 0.7)  # Default to 0.7 if not specified
        }
        return params

    def get_result_parser_params(self) -> dict:
        """Generates a params dict for the result parser based on the config."""
        result_parser_params = self.config.get('result_parser_params', {})
        params = { #TODO: Pretty sure this whole thing also needs a reformat, just doing it this way to save time for now
            "dumps": [
                {
                    "filename":         "all_discovered_motifs.csv", #TODO these need to be in a var or const somewhere
                    "only_matches":     False,
                    "only_significant": False
                },
                {
                    "filename":         "matched_discovered_motifs.csv",
                    "only_matches":     True,
                    "only_significant": False
                },
                {
                    "filename":         "significant_discovered_motifs.csv",
                    "only_matches":     False,
                    "only_significant": True
                }
            ],
        }
        return params

    def get_heatmaps_generator_params(self) -> dict:
        """Generates a params dict for the heatmaps generator based on the config."""
        denovo_params = self.get_denovo_params()
        dataset_generation_params = self.get_dataset_generation_params()
        parsed_results = self.get_parsed_results()
        
        params = {
            # Output directory for heatmaps
            "output_dir": f"{self.output_dir}/heatmaps",
            
            # Experimental design parameters
            "seq_amounts": dataset_generation_params.get('seq_amounts', []),
            "injection_rates": dataset_generation_params.get('injection_rates', []),
            "n_replicates": dataset_generation_params.get('n_replicates', 1),
            
            # Tools and backgrounds tested (include GimmeMotifs)
            "tools": denovo_params.get('tools', []) + ["GimmeMotifs"],
            "backgrounds": denovo_params.get('background_types', []),
            
            # Parsed CSV file paths
            "parsed_results": {
                "all": parsed_results.get('all_discovered_motifs.csv', {}),
                "matched": parsed_results.get('matched_discovered_motifs.csv', {}),
                "significant": parsed_results.get('significant_discovered_motifs.csv', {})
            }
        }
        return params

    def get_report_params(self) -> dict:
        """
        Return all the settings needed to generate the Markdown report in one shot.
        """
        denovo_params = self.get_denovo_params()
        dataset_generation_params = self.get_dataset_generation_params()
        match_params = self.get_match_params()
        parsed_results = self.get_parsed_results()
        heatmaps_params = self.get_generated_heatmap()
    
        return {
            # 1. Where to write the report
            "output_dir": os.path.join(self.get_output_dir(), "reports"),
            "report_filename": "report.md",

            # 2. Experimental design
            "seq_amounts": dataset_generation_params.get('seq_amounts', []),
            "injection_rates": dataset_generation_params.get('injection_rates', []),
            "n_replicates": dataset_generation_params.get('n_replicates', 1),

            # 3. Tools & backgrounds tested. must append GimmeMotifs to the tools list.
            "tools": denovo_params.get('tools', []) + ["GimmeMotifs"],
            "backgrounds": denovo_params.get('background_types', []),  # e.g. ["genomic", "gc", "random", "true_random"]

            # 4. Success threshold (for “sweet-spot” analyses)
            "threshold": match_params.get('min_score'),

            # 5. Parsed-CSV metadata (as returned by get_parsed_results())
            "parsed_results": {
                "all":      parsed_results.get('all_discovered_motifs.csv', {}),
                "matched":  parsed_results.get('matched_discovered_motifs.csv', {}),
                "significant": parsed_results.get('significant_discovered_motifs.csv', {}),
            },

            # 6. Heatmap-image metadata (as returned by get_generated_heatmap())
            "heatmaps": {
                "all": {"path": heatmaps_params.get('all', {}).get('path', ""), "only_significant": False},
                "sig": {"path": heatmaps_params.get('sig', {}).get('path', ""), "only_significant": True}
            }
        }


# if __name__ == "__main__":
#     # Example usage
#     config_path = "/polio/oded/MotiFabEnv/presentation_run/dataset_config.json" #TODO PROBABLY should avoid publishing this path online
#     dataset_manager = DatasetManager(config_path)
#     print(dataset_manager.report_missing_outputs())
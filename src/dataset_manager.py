import os
import json
from typing import List, Optional
from gimmemotifs_plus.fasta_plus import FastaPlus
from gimmemotifs_plus.motif_plus import MotifPlus

class DatasetManager:
    def __init__(self, config_path: str):
        """Initialize DatasetManager with JSON config file.
        if the file does not exist, it will be created as an empty json file."""
        self.config_path = config_path
        if not os.path.exists(config_path):
            # initialize config with nested generation params
            self.config = {'dataset_generation_params': {}}
            with open(config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
        else:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        # ensure nested params exists
        self.config.setdefault('dataset_generation_params', {})

    def _generate_combination_name(self, seq_amount: int, injection_rate: float) -> str:
        """Generate a unique name for the combination based on seq_amount and injection_rate."""
        return f"len_{seq_amount}_rate_{int(injection_rate * 100)}"
    
    def _generate_combination_path(self, seq_amount: int, injection_rate: float) -> str:
        """Generate a dir path for the combination based on seq_amount and injection_rate."""
        name = self._generate_combination_name(seq_amount, injection_rate)
        params = self.config.get('dataset_generation_params', {})
        return os.path.join(params.get('output_dir', ''), name)
    
    def _get_combination(self, seq_amount: int, injection_rate: float) -> Optional[dict]:
        """Retrieve a combination entry by seq_amount and injection_rate."""
        key = self._generate_combination_name(seq_amount, injection_rate)
        return self.config.get('combinations', {}).get(key)
    
    def _generate_replicate_name(self, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a name for a replicate based on seq_amount, injection_rate, and replicate number."""
        return f"{self._generate_combination_name(seq_amount, injection_rate)}_rep_{replicate}"
    
    def _generate_replicate_dir_path(self, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a directory path for a replicate based on seq_amount, injection_rate, and replicate number."""
        combination_path = self._generate_combination_path(seq_amount, injection_rate)
        return os.path.join(combination_path, f"rep_{replicate}")
    
    def _generate_replicate_file_path(self, seq_amount: int, injection_rate: float, replicate: int) -> str:
        """Generate a file path for a replicate based on seq_amount, injection_rate, and replicate number."""
        replicate_dir = self._generate_replicate_dir_path(seq_amount, injection_rate, replicate)
        return os.path.join(replicate_dir, f"{self._generate_replicate_name(seq_amount, injection_rate, replicate)}.fa")
    
    def _get_replicate(self, seq_amount: int, injection_rate: float, replicate: int) -> Optional[dict]:
        """Retrieve a replicate entry by seq_amount, injection_rate, and replicate number."""
        combination = self._get_combination(seq_amount, injection_rate)
        if combination:
            return combination.get('replicates', {}).get(f"rep_{replicate}")
        return None
    
    def _generate_combination(self, fastap: FastaPlus, motifp: MotifPlus, seq_amount: int, injection_rate: float, force = False):
        # TODO: implement force logic to avoid generating existing combinations
        # retrieve generation parameters
        params = self.config.get('dataset_generation_params', {})
        background_length = params.get('background_length', None)
        n_replicates = params.get('n_replicates', 1)
        injected_fastaps, background_fastap = fastap.create_non_overlapping_injected_subsets(
            motif=motifp,
            seq_amount=seq_amount,
            replicates=n_replicates,
            background_length=background_length,
            injection_rate=injection_rate,
        )
        combination_name = self._generate_combination_name(seq_amount, injection_rate)
        combination_path = self._generate_combination_path(seq_amount, injection_rate)
        #create the combination directory if it does not exist
        os.makedirs(combination_path, exist_ok=True)
        # if background was created in the previous step, generate a background FASTA file
        background_path = None
        if background_fastap:
            background_path = os.path.join(combination_path, f"{combination_name}_background.fa")
            background_fastap.writefasta(background_path)
        
        combination = {
            'name': combination_name,
            'dir': combination_path,
            'seq_amount': seq_amount,
            'injection_rate': injection_rate,
            'n_replicates': n_replicates,
            'background_path': background_path,
            'replicates': {},
            'status': 'generated'
        }
        # Generate replicate entries
        for i, injected_fastap in enumerate(injected_fastaps, start=1):
            replicate_name = self._generate_replicate_name(seq_amount, injection_rate, i)
            replicate_dir = self._generate_replicate_dir_path(seq_amount, injection_rate, i)
            replicate_file_path = self._generate_replicate_file_path(seq_amount, injection_rate, i)
            os.makedirs(replicate_dir, exist_ok=True)
            injected_fastap.writefasta(replicate_file_path)
            combination['replicates'][f"rep_{i}"] = {
                'name': replicate_name,
                'dir': replicate_dir,
                'test_fasta': replicate_file_path,
                'status': 'generated'
            }
        # Save combination to config
        if 'combinations' not in self.config:
            self.config['combinations'] = {}
        self.config['combinations'][combination_name] = combination

    def generate_datasets(
        self,
        master_fasta: Optional[str] = None,
        genome_fasta: Optional[str] = None,
        output_dir: Optional[str] = None,
        seq_amounts: Optional[List[str]] = None,
        injection_rates: Optional[List[str]] = None,
        n_replicates: Optional[int] = None,
        background_length: Optional[int] = None,
        pfm: Optional[str] = None,
        ppm: Optional[str] = None,
        consensus: Optional[str] = None,
        mutation_rate: Optional[float] = None,
        force: bool = False
    ):
        """
        Generate datasets based on configuration and provided parameters.
        Parameters overwrite JSON values if given.
        """
        # Merge provided parameters into nested config
        params = self.config.get('dataset_generation_params')
        for key, val in [
            ('master_fasta', master_fasta),
            ('genome_fasta', genome_fasta),
            ('output_dir', output_dir),
            ('seq_amounts', seq_amounts),
            ('injection_rates', injection_rates),
            ('n_replicates', n_replicates),
            ('background_length', background_length),
            ('pfm', pfm),
            ('ppm', ppm),
            ('consensus', consensus),
            ('mutation_rate', mutation_rate)
        ]:
            if val is not None:
                params[key] = val

        # Validations
        # validation context from nested params
        master = params.get('master_fasta')
        if not master:
            raise ValueError("master_fasta must be provided via configuration or parameter")
        if not os.path.exists(master):
            raise FileNotFoundError(f"master_fasta not found: {master}")

        genome = params.get('genome_fasta')
        if genome and not os.path.exists(genome):
            raise FileNotFoundError(f"genome_fasta not found: {genome}")

        # Exactly one of pfm, ppm, consensus must be provided
        sources = [
            bool(params.get('pfm')),
            bool(params.get('ppm')),
            bool(params.get('consensus'))
        ]
        if sum(sources) != 1:
            raise ValueError("Exactly one of pfm, ppm, or consensus must be provided")

        # Required parameters
        # required generation params
        for param in ['output_dir', 'seq_amounts', 'injection_rates', 'n_replicates']:
            val = params.get(param)
            if not val:
                raise ValueError(f"{param} must be provided via configuration or parameter")

        # background types must not be empty and all values must be within the allowed set:
        # 'random'
        # 'genomic' - requires genome_fasta
        # 'gc' - requires genome_fasta
        # 'custom' - requires background_length
        # allowed_bg_types = {'random', 'genomic', 'gc', 'custom'}
        # bg_types = params.get('background_types', [])
        # if not bg_types or not all(t in allowed_bg_types for t in bg_types):
        #     raise ValueError(f"background_types must be one of: {', '.join(sorted(allowed_bg_types))}")
        # if 'genomic' in bg_types and not genome:
        #     raise ValueError("genomic background type requires genome_fasta to be provided")
        # if 'gc' in bg_types and not genome:
        #     raise ValueError("gc background type requires genome_fasta to be provided")
        # if 'custom' in bg_types and not params.get('background_length'):
        #     raise ValueError("custom background type requires background_length to be provided")

        # instantiate generators
        master_fastap = FastaPlus(fname=master)
        motifp = MotifPlus(
            pfm=params.get('pfm'),
            ppm=params.get('ppm'),
            consensus=params.get('consensus'),
            mutation_rate=params.get('mutation_rate', 0.0)
        )
        
        if not self.config.get('combinations'):
            self.config['combinations'] = {}
            
        # Ensure output directory exists
        output_dir = params.get('output_dir')
        if not output_dir:
            raise ValueError("output_dir must be provided via configuration or parameter")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        elif not os.path.isdir(output_dir):
            raise ValueError(f"output_dir must be a directory, got {output_dir}")
            
        # Generate combinations for each seq_amount and injection_rate
        for seq_amount in params.get('seq_amounts', []):
            if not isinstance(seq_amount, int) or seq_amount <= 0:
                raise ValueError(f"seq_amount must be a positive integer, got {seq_amount}")
            for injection_rate in params.get('injection_rates', []):
                if not isinstance(injection_rate, (int, float)) or not (0.0 <= injection_rate <= 1.0):
                    raise ValueError(f"injection_rate must be between 0 and 1, got {injection_rate}")
                # Generate the combination
                self._generate_combination(
                    fastap=master_fastap,
                    motifp=motifp,
                    seq_amount=seq_amount,
                    injection_rate=injection_rate,
                    force=force
                )
        # Save updated config
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=4)
        print(f"Generated datasets saved to {self.config_path}")
        return self.config['combinations']
        
# Example usage:
if __name__ == "__main__":
    manager = DatasetManager('/polio/oded/MotiFabEnv/gimmeMotifs_py_testing/dataset_config.json')
    combinations = manager.generate_datasets(
    )
    print("Generated combinations:")
    for name, entry in combinations.items():
        print(f"{name}: {entry['seq_amount']} sequences, {entry['injection_rate']} injection rate")
        for rep_name, rep in entry['replicates'].items():
            print(f"  {rep_name}: {rep['test_fasta']}")
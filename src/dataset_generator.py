from dataset_manager import DatasetManager
import os
import json
from gimmemotifs_plus.fasta_plus import FastaPlus
from gimmemotifs_plus.motif_plus import MotifPlus
from typing import List, Optional
class DatasetGenerator:
    def __init__(self, dataset_manager: DatasetManager):
        self.dataset_manager = dataset_manager
        self.params = dataset_manager.get_dataset_generation_params()
        
    def generate_combo(self, fastap: FastaPlus, motifp: MotifPlus, seq_amount: int, injection_rate: float, force: bool = False):
        """
        Generate a single combination unless force=False and it already exists.
        """
        mngr = self.dataset_manager
        combo_name = mngr.get_combo_name(seq_amount, injection_rate)
        existing = mngr.get_combo(seq_amount, injection_rate)
        # Skip if already generated and no force flag
        if existing and not force and not existing.get('force', False):
            print(f"Skipping existing combination {combo_name} (use force to override)")
            return existing
        # retrieve generation parameters
        params = self.params
        out_dir = params.get('output_dir', '.')
        background_length = params.get('background_length', None)
        n_replicates = params.get('n_replicates', 1)
        injected_fastaps, background_fastap = fastap.create_non_overlapping_injected_subsets(
            motif=motifp,
            seq_amount=seq_amount,
            replicates=n_replicates,
            background_length=background_length,
            injection_rate=injection_rate,
        )
        combination_name = mngr.get_combo_name(seq_amount, injection_rate)
        combination_path = mngr.get_combo_path(out_dir, seq_amount, injection_rate)
        #create the combination directory if it does not exist
        os.makedirs(combination_path, exist_ok=True)
        # if background was created in the previous step, generate a background FASTA file
        background_path = None
        if background_fastap:
            background_path = os.path.join(combination_path, "background.fa")
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
            replicate_name = mngr.get_rep_name(seq_amount, injection_rate, i)
            replicate_dir = mngr.get_rep_dir_path(out_dir, seq_amount, injection_rate, i)
            gimme_out_dir = os.path.join(replicate_dir, 'gimme_out')
            replicate_file_path = mngr.get_rep_file_path(out_dir, seq_amount, injection_rate, i)
            os.makedirs(replicate_dir, exist_ok=True)
            injected_fastap.writefasta(replicate_file_path)
            combination['replicates'][replicate_name] = {
                'name': replicate_name,
                'dir': replicate_dir,
                'gimme_out_dir': gimme_out_dir,
                'test_fasta': replicate_file_path,
                'background_fasta': background_path if background_path else None,
                'status': 'generated',
            }
        # Save combination to manager
        mngr.upsert_combo(seq_amount, injection_rate, combination)
        print(f"Generated combination: {combination_name} with {n_replicates} replicates at {injection_rate} injection rate")
        return combination
        
    def generate_datasets(
        self,
        master_fasta: Optional[str] = None,
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
        params = self.params
        for key, val in [
            ('master_fasta', master_fasta),
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
        master = self.dataset_manager.get_master_fasta()
        if not master:
            raise ValueError("master_fasta must be provided via configuration or parameter")
        if not os.path.exists(master):
            raise FileNotFoundError(f"master_fasta not found: {master}")

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
            
        #if we pass validations, need to update the config with these values
        self.dataset_manager.update_dataset_generation_params(params) #TODO the hell is this on about? might need to remove completely
        #TODO: this might need to be handled differently since we are overwriting the config

        # instantiate generators
        master_fastap = FastaPlus(fname=master)
        motifp = self.dataset_manager.get_motifp()  # Retrieve motif as MotifPlus object
        if not motifp:
            raise ValueError("No valid motif data found")
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
                self.generate_combo(
                    fastap=master_fastap,
                    motifp=motifp,
                    seq_amount=seq_amount,
                    injection_rate=injection_rate,
                    force=force
                )
        return self.dataset_manager.get_combos_dict()
   
   
   # Example usage:
# if __name__ == "__main__":
#     manager = DatasetManager('/polio/oded/MotiFabEnv/presentation_run/dataset_config.json')
#     generator = DatasetGenerator(manager)
#     combinations = generator.generate_datasets()
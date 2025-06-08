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
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=4)
            
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

    def get_dataset_generation_params(self) -> dict:
        """Retrieve the dataset generation parameters from the config."""
        return self.config.get('dataset_generation_params', {})
    
    def update_dataset_generation_params(self, params: dict) -> None:
        """Update the dataset generation parameters in the config."""
        self.config['dataset_generation_params'] = params
        # Save the updated config to file
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=4)
        #TODO: think of a better way to handle this, since this will overwrite any existing params
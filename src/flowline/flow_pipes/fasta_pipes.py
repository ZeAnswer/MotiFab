import random
from flowline import FlowPipe

class LoadFastaPipe(FlowPipe):
    """
    FlowPipe to load a FASTA file and parse its contents into a list of sequence records.

    Inputs:
        - "fasta_file_path" (str): Path to the FASTA file.

    Outputs:
        - "fasta_records" (list): A list of dictionaries with:
            - "id" (str): The sequence identifier (first word after ">").
            - "desc" (str): The full header line (without ">").
            - "seq" (str): The concatenated sequence.

    Raises:
        - ValueError if the file does not start with a header line.
    """
    def __init__(self):
        super().__init__(
            inputs=["fasta_file_path"], 
            outputs=["fasta_records"], 
            action=self.load_fasta
        )
    
    def load_fasta(self, data):
        file_path = data["fasta_file_path"]
        records = []
        current_record = None

        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith(">"):
                        if current_record is not None:
                            records.append(current_record)
                        header = line[1:]
                        parts = header.split(maxsplit=1)
                        record_id = parts[0]
                        record_desc = header
                        current_record = {"id": record_id, "desc": record_desc, "seq": ""}
                    else:
                        if current_record is None:
                            raise ValueError("FASTA file does not start with a header line.")
                        current_record["seq"] += line.strip()
                if current_record is not None:
                    records.append(current_record)
        except Exception as e:
            raise RuntimeError(f"Error loading FASTA file '{file_path}': {e}")

        return {"fasta_records": records}


class WriteFastaPipe(FlowPipe):
    """
    FlowPipe to write a list of FASTA records to a file.

    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
        - "fasta_file_path" (str): Path to write the FASTA file.

    Outputs:
        - "write_success" (bool): Whether the operation was successful.
        - "fasta_file_path" (str): The path to the written file.
    """

    def __init__(self, fasta_file_path = None):
        super().__init__(inputs=["fasta_records", "fasta_file_path"], outputs=["write_success"], action=self.write_fasta)
        # Store default value as instance variable
        self.default_fasta_file_path = fasta_file_path
        
        # Register which inputs have default values
        optional_inputs = []
        if fasta_file_path is not None:
            optional_inputs.append("fasta_file_path")
        self.set_optional_inputs(optional_inputs)

    def write_fasta(self, data):
        records = data["fasta_records"]
        output_path = data.get("fasta_file_path", self.default_fasta_file_path)

        try:
            with open(output_path, 'w') as f:
                for record in records:
                    f.write(f">{record['desc']}\n")
                    seq = record["seq"]
                    for i in range(0, len(seq), 80):
                        f.write(seq[i:i+80] + "\n")
        except Exception as e:
            raise RuntimeError(f"Error writing FASTA file to '{output_path}': {e}")

        return {"write_success": True, "fasta_file_path": output_path}


import random

class SelectRandomFastaSequencesPipe(FlowPipe):
    """
    FlowPipe to select a random subset of FASTA records, allowing duplicates if necessary.

    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
        - "amount" (int): The number of records to select.
        - "excluded_indices" (list, optional): Indices that must not be selected.
        - "mandatory_indices" (list, optional): Indices that must always be included (once).

    Outputs:
        - "fasta_records" (list): A randomly selected subset of records (duplicates allowed if needed).
        - "indices" (list): The indices of the selected records.

    Raises:
        - ValueError if mandatory indices are out of range.
        - ValueError if excluded indices are out of range.
        - ValueError if mandatory indices and excluded indices overlap.
    """

    def __init__(self, amount=None, excluded_indices=None, mandatory_indices=None):
        super().__init__(
            inputs=["fasta_records", "amount", "excluded_indices", "mandatory_indices"],
            outputs=["fasta_records", "indices"],
            action=self.select_random_sequences
        )
        # Store default values as instance variables
        self.default_amount = amount
        self.default_excluded_indices = excluded_indices or []
        self.default_mandatory_indices = mandatory_indices or []
        
        # Register which inputs have default values
        optional_inputs = []
        optional_inputs.append("excluded_indices")
        optional_inputs.append("mandatory_indices")
        if amount is not None:
            optional_inputs.append("amount")
        self.set_optional_inputs(optional_inputs)

    def select_random_sequences(self, data):
        records = data["fasta_records"]
        count = data.get("amount", self.default_amount)
        total_records = len(records)

        excluded_indices = set(data.get("excluded_indices", self.default_excluded_indices))  # Set for fast lookup
        mandatory_indices = set(data.get("mandatory_indices", self.default_mandatory_indices))  # Set for fast lookup

        # --- Validate Record Count ---
        if total_records == 0:
            raise ValueError("No records available for selection.")

        # --- Validate Mandatory Indices ---
        out_of_range_mandatory = {i for i in mandatory_indices if i < 0 or i >= total_records}
        if out_of_range_mandatory:
            raise ValueError(
                f"Mandatory indices {sorted(out_of_range_mandatory)} are out of range (valid range: 0-{total_records - 1})."
            )

        # --- Ensure Mandatory & Excluded Indices Don't Overlap ---
        conflicting_indices = mandatory_indices & excluded_indices
        if conflicting_indices:
            raise ValueError(
                f"Mandatory indices {sorted(conflicting_indices)} are also in the excluded list. Remove these from either set."
            )
            
        # --- Validate Mandatory Indices Amount ---
        if len(mandatory_indices) > count:
            raise ValueError(
                f"Number of mandatory indices ({len(mandatory_indices)}) exceeds the requested selection count ({count})."
            )

        # --- Calculate Available Indices ---
        available_indices = set(range(total_records)) - excluded_indices
        available_indices_without_mandatory =available_indices - mandatory_indices  # Ensure mandatory indices appear exactly once

        # --- Base Selection ---
        selected_indices = list(mandatory_indices)  # Include mandatory indices first
        remaining_needed = count - len(selected_indices)

        # --- Fill Remaining Needed ---
        # First we select from the available list not including mandatory indices. if we need more than available, we will add duplicates including mandatory indices.
        if remaining_needed > 0:
            selected_indices.extend(random.sample(list(available_indices_without_mandatory), min(remaining_needed, len(available_indices_without_mandatory))))
            remaining_needed = count - len(selected_indices)
        
        # --- Add Duplicates ---
        if remaining_needed > 0:
            selected_indices.extend(random.choices(selected_indices, k=remaining_needed))
        

        selected_records = [records[i] for i in selected_indices]

        return {"fasta_records": selected_records, "indices": selected_indices}
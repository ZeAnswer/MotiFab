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

    External Inputs:
        - "fasta_file_path"

    Raises:
        - ValueError if the file does not start with a header line.
    """
    __doc__ = __doc__ 
    def __init__(self, file_path = None):
        super().__init__(inputs=["fasta_file_path"], outputs=["fasta_records"], action=self.load_fasta, external_inputs={"fasta_file_path": file_path})

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
        
    External Inputs:
        - "fasta_file_path"
    """

    def __init__(self, file_path = None):
        super().__init__(inputs=["fasta_records", "fasta_file_path"], outputs=["write_success"], action=self.write_fasta, external_inputs={"fasta_file_path": file_path})

    def write_fasta(self, data):
        records = data["fasta_records"]
        output_path = data["fasta_file_path"]

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


class SelectRandomFastaSequencesPipe(FlowPipe):
    """
    FlowPipe to select a random subset of FASTA records.

    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
        - "amount" (int): The amount of records to select.

    Outputs:
        - "fasta_records" (list): A randomly selected subset of records.
        - "indices" (list): The indices of the selected records.
        
    External Inputs:
        - "amount"

    Raises:
        - ValueError if the requested count is greater than the available records.
    """

    def __init__(self, amount = None):
        super().__init__(inputs=["fasta_records", "amount"], outputs=["fasta_records", "indices"], action=self.select_random_sequences, external_inputs={"amount": amount})

    def select_random_sequences(self, data):
        records = data["fasta_records"]
        count = data["amount"]

        if count > len(records):
            raise ValueError("Requested count exceeds the amount of available records.")

        indices = random.sample(range(len(records)), count)
        selected_records = [records[i] for i in indices]
        
        return {"fasta_records": selected_records, "indices": indices}
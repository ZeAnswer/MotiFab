import random
from flowline import FlowPipe

class NaiveShufflePipe(FlowPipe):
    """
    FlowPipe to perform a naive nucleotide sequence shuffle on sequences in FASTA records.
    
    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
    
    Outputs:
        - "fasta_records" (list): The FASTA records with shuffled sequences.
    
    Raises:
        - ValueError if the input is empty or contains invalid sequences.
    """
    def __init__(self):
        super().__init__(inputs=["fasta_records"], outputs=["fasta_records"], action=self.naive_shuffle)

    def naive_shuffle(self, data):
        fasta_records = data["fasta_records"]
        
        if not fasta_records:
            raise ValueError("Input FASTA records list cannot be empty.")
        
        shuffled_records = []
        for record in fasta_records:
            new_record = record.copy()
            seq = record["seq"]
            
            if not seq:
                raise ValueError("A sequence in the FASTA records is empty.")
                
            seq_list = list(seq)
            random.shuffle(seq_list)
            new_record["seq"] = ''.join(seq_list)
            
            shuffled_records.append(new_record)
            
        return {"fasta_records": shuffled_records}

    def __str__(self):
        """Returns a debug-friendly representation of the NaiveShufflePipe."""
        return "NaiveShufflePipe(Shuffling sequences in FASTA records)"


class DiPairShufflePipe(FlowPipe):
    """
    FlowPipe to perform a dinucleotide-pair shuffle on sequences in FASTA records.
    
    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
    
    Outputs:
        - "fasta_records" (list): The FASTA records with dinucleotide-pair shuffled sequences.
    
    Raises:
        - ValueError if the input is empty or contains invalid sequences.
    """
    def __init__(self):
        super().__init__(inputs=["fasta_records"], outputs=["fasta_records"], action=self.di_pair_shuffle)

    def di_pair_shuffle(self, data):
        fasta_records = data["fasta_records"]
        
        if not fasta_records:
            raise ValueError("Input FASTA records list cannot be empty.")
        
        shuffled_records = []
        for record in fasta_records:
            new_record = record.copy()
            seq = record["seq"]
            
            if not seq:
                raise ValueError("A sequence in the FASTA records is empty.")
                
            # Partition the sequence into dinucleotide blocks.
            pairs = [seq[i:i+2] for i in range(0, len(seq) - 1, 2)]
            remainder = seq[-1] if len(seq) % 2 != 0 else None
            blocks = pairs + ([remainder] if remainder else [])
            
            random.shuffle(blocks)
            new_record["seq"] = ''.join(blocks)
            
            shuffled_records.append(new_record)
            
        return {"fasta_records": shuffled_records}

    def __str__(self):
        """Returns a debug-friendly representation of the DiPairShufflePipe."""
        return "DiPairShufflePipe(Shuffling sequences in FASTA records in dinucleotide pairs)"
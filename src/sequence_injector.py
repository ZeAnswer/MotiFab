import random

def inject_motif(seq: str, motif: str) -> str:
    """
    Injects the motif into the given sequence by replacing a substring at a random position.
    
    If the sequence length is less than the motif length, returns the original sequence.
    
    Args:
        seq (str): The original sequence.
        motif (str): The motif to inject.
        
    Returns:
        str: The sequence with the motif injected.
    """
    if len(seq) < len(motif):
        return seq
    pos = random.randint(0, len(seq) - len(motif))
    return seq[:pos] + motif + seq[pos + len(motif):] #TODO: add injection options i.e. replace/insert

def inject_motif_into_records(records: list, motif: str, injection_rate) -> list:
    """
    Injects the given motif into a subset of FASTA records.
    
    The injection_rate parameter can be either:
      - An absolute integer specifying how many records (out of the total) to inject.
      - A percentage string (e.g., "10%") specifying the relative number of records to inject.
    
    Args:
        records (list): A list of FASTA record dictionaries (each with a "seq" key).
        motif (str): The motif string to inject.
        injection_rate: Either an integer or a string ending with "%" to indicate the fraction.
        
    Returns:
        list: A new list of FASTA records with the motif injected in the selected records.
    """
    num_records = len(records)
    
    # Determine injection_count based on the type of injection_rate.
    if isinstance(injection_rate, str) and injection_rate.endswith("%"):
        try:
            percentage = float(injection_rate.rstrip("%"))
            injection_count = int(round(num_records * percentage / 100))
        except Exception as e:
            raise ValueError(f"Invalid injection rate format: {injection_rate}") from e
    else:
        injection_count = int(injection_rate)
    
    if injection_count > num_records:
        injection_count = num_records

    # Randomly choose which records to inject.
    injection_indices = random.sample(range(num_records), injection_count)
    new_records = []
    for i, rec in enumerate(records):
        new_rec = rec.copy()
        if i in injection_indices:
            new_rec["seq"] = inject_motif(new_rec["seq"], motif)
        new_records.append(new_rec)
    return new_records
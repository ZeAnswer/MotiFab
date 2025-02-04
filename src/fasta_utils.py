import random

def load_fasta(file_path: str) -> list:
    """
    Reads a FASTA file and returns a list of sequence records.
    
    Each record is a dictionary with keys:
      - "id": the identifier (first word after '>')
      - "desc": the full header (without the '>' character)
      - "seq": the concatenated sequence (all subsequent lines until the next header)
    
    Args:
        file_path (str): Path to the FASTA file.
    
    Returns:
        list: A list of dictionaries, one per FASTA record.
    
    Raises:
        ValueError: If the file does not start with a header line.
    """
    records = []
    current_record = None

    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            if line.startswith(">"):
                if current_record is not None:
                    records.append(current_record)
                header = line[1:]
                parts = header.split(maxsplit=1)
                record_id = parts[0]
                record_desc = header  # Use full header as description.
                current_record = {"id": record_id, "desc": record_desc, "seq": ""}
            else:
                if current_record is None:
                    raise ValueError("FASTA file does not start with a header line.")
                current_record["seq"] += line.strip()
        if current_record is not None:
            records.append(current_record)
    return records

def write_fasta(records: list, output_path: str) -> None:
    """
    Writes a list of FASTA records to an output file in FASTA format.
    
    Each record should be a dictionary with at least the keys "id" and "seq".
    
    Args:
        records (list): A list of FASTA record dictionaries.
        output_path (str): The file path to write the FASTA output.
    """
    with open(output_path, 'w') as f:
        for record in records:
            f.write(f">{record['desc']}\n")
            # Wrap sequence every 80 characters for readability.
            seq = record["seq"]
            for i in range(0, len(seq), 80):
                f.write(seq[i:i+80] + "\n")

def select_random_sequences(records: list, count: int) -> list:
    """
    Selects a random subset (without replacement) of FASTA records.
    
    Args:
        records (list): A list of FASTA record dictionaries.
        count (int): The number of records to select.
    
    Returns:
        list: A list of randomly selected records.
    
    Raises:
        ValueError: If count is greater than the number of available records.
    """
    if count > len(records):
        raise ValueError("Requested count exceeds the number of available records.")
    return random.sample(records, count)

# ----------------------------
# New Functions: Random FASTA Generation
# ----------------------------

def generate_random_sequence(length: int) -> str:
    """
    Generates a random nucleotide sequence of the specified length.
    
    Args:
        length (int): Length of the sequence.
    
    Returns:
        str: A random sequence composed of A, C, G, and T.
    """
    if length <= 0:
        raise ValueError("Sequence length must be positive.")
    nucleotides = "ACGT"
    return ''.join(random.choices(nucleotides, k=length))

def generate_random_fasta_records(num_sequences: int = 10,
                                  min_length: int = 50,
                                  max_length: int = 100,
                                  prefix: str = "seq") -> list:
    """
    Generates a list of random FASTA records.
    
    Each record is a dictionary with keys "id", "desc", and "seq". The sequence length 
    for each record is randomly chosen between min_length and max_length (inclusive). The record
    identifier and description are prefixed with the given prefix and an index.
    
    Args:
        num_sequences (int): Number of records to generate (default: 10).
        min_length (int): Minimum sequence length (default: 50).
        max_length (int): Maximum sequence length (default: 100).
        prefix (str): Prefix for the sequence identifier and description (default: "seq").
    
    Returns:
        list: A list of random FASTA record dictionaries.
    
    Raises:
        ValueError: If min_length is greater than max_length.
    """
    if min_length > max_length:
        raise ValueError("min_length cannot be greater than max_length.")
    records = []
    for i in range(1, num_sequences + 1):
        length = random.randint(min_length, max_length)
        seq = generate_random_sequence(length)
        record_id = f"{prefix}{i}"
        record_desc = f"{prefix}{i}"  # For simplicity, using the same for id and description.
        record = {"id": record_id, "desc": record_desc, "seq": seq}
        records.append(record)
    return records

def generate_random_fasta_file(output_path: str,
                               num_sequences: int = 10,
                               min_length: int = 50,
                               max_length: int = 100,
                               prefix: str = "seq") -> None:
    """
    Generates a random FASTA file with the given parameters.
    
    Args:
        output_path (str): The path to write the generated FASTA file.
        num_sequences (int): Number of sequences to generate (default: 10).
        min_length (int): Minimum sequence length (default: 50).
        max_length (int): Maximum sequence length (default: 100).
        prefix (str): Prefix for each sequence identifier (default: "seq").
    """
    records = generate_random_fasta_records(num_sequences, min_length, max_length, prefix)
    write_fasta(records, output_path)
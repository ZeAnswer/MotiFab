import random
from flowline import FlowPipe

# A small pseudocount to avoid zero (or extremely low) probabilities.
PSEUDOCOUNT = 0.0001


class GenerateRandomMotifsPipe(FlowPipe):
    """
    FlowPipe to generate a list of random DNA motifs.

    Inputs:
        - "amount" (int): The amount of motifs to generate.
        - "length" (int): The length of each generated motif.

    Outputs:
        - "motif_strings" (list): A list of randomly generated motif strings.

    External Inputs:
        - "amount"
        - "length"

    Raises:
        - ValueError if the motif length or amount of motifs is not positive.
    """

    def __init__(self, amount=None, length=None):
        super().__init__(
            inputs=["amount", "length"],
            outputs=["motif_strings"],
            action=self.generate_random_motifs,
            external_inputs={"amount": amount, "length": length},
        )

    def generate_random_motifs(self, data):
        num_motifs = data["amount"]
        motif_length = data["length"]

        if num_motifs <= 0:
            raise ValueError("The amount of motifs must be a positive integer.")

        if motif_length <= 0:
            raise ValueError("Motif length must be a positive integer.")

        nucleotides = "ACGT"
        motifs = [''.join(random.choice(nucleotides) for _ in range(motif_length)) for _ in range(num_motifs)]
        
        return {"motif_strings": motifs}

    def __str__(self):
        """Returns a debug-friendly representation of the GenerateRandomMotifsPipe."""
        return "GenerateRandomMotifsPipe(Generating multiple random motifs)"


class ParsePWMPipe(FlowPipe):
    """
    FlowPipe to parse a Position Weight Matrix (PWM) file.

    Inputs:
        - "pwm_file_path" (str): Path to the PWM file.

    Outputs:
        - "pwm_matrix" (dict): A dictionary containing nucleotide probabilities:
            - Keys: "A", "C", "G", "T"
            - Values: Lists of floats representing probabilities at each motif position.

    Raises:
        - FileNotFoundError if the file does not exist.
        - ValueError if the file format is incorrect or the matrix is invalid.
    """

    def __init__(self):
        super().__init__(inputs=["pwm_file_path"], outputs=["pwm_matrix"], action=self.parse_pwm_file)

    def parse_pwm_file(self, data):
        file_path = data["pwm_file_path"]
        try:
            with open(file_path, 'r') as f:
                raw_lines = f.readlines()
        except FileNotFoundError:
            raise FileNotFoundError(f"PWM file not found: {file_path}")

        lines = [line.strip() for line in raw_lines if line.strip() != '']

        if not lines or not lines[0].startswith("#INCLUSive Motif Model"):
            raise ValueError("PWM file does not start with the required header '#INCLUSive Motif Model'.")

        motif_width = None
        index = 1
        while index < len(lines) and lines[index].startswith("#"):
            line = lines[index]
            if line.startswith("#W"):
                parts = line.split("=")
                if len(parts) < 2:
                    raise ValueError("Malformed '#W' line in PWM file.")
                try:
                    motif_width = int(parts[1].strip())
                except Exception as e:
                    raise ValueError(f"Error parsing motif width: {e}")
            index += 1

        if motif_width is None:
            raise ValueError("PWM file is missing the required '#W' (motif width) line.")

        matrix_lines = lines[index:]
        if not matrix_lines:
            raise ValueError("PWM file contains no matrix data.")

        matrix = []
        for line in matrix_lines:
            parts = line.split()
            if len(parts) != 4:
                raise ValueError(f"Expected 4 entries per matrix line, got {len(parts)} in line: '{line}'")
            try:
                row = [float(x) for x in parts]
            except ValueError:
                raise ValueError(f"Non-numeric value encountered in matrix line: '{line}'")
            matrix.append(row)

        if len(matrix) == 4:
            matrix = list(map(list, zip(*matrix)))

        if len(matrix) != motif_width:
            raise ValueError(f"Number of matrix rows ({len(matrix)}) does not match motif width ({motif_width}).")

        normalized_matrix = []
        for row in matrix:
            new_row = [value + PSEUDOCOUNT for value in row]
            total = sum(new_row)
            if total == 0:
                raise ValueError("Row sums to zero after adding pseudocounts; cannot normalize.")
            normalized_row = [v / total for v in new_row]
            normalized_matrix.append(normalized_row)

        pwm_dict = {"A": [], "C": [], "G": [], "T": []}
        for row in normalized_matrix:
            pwm_dict["A"].append(row[0])
            pwm_dict["C"].append(row[1])
            pwm_dict["G"].append(row[2])
            pwm_dict["T"].append(row[3])

        return {"pwm_matrix": pwm_dict}


class SampleMotifsFromPWMPipe(FlowPipe):
    """
    FlowPipe to sample multiple motif sequences from a Position Weight Matrix (PWM).

    Inputs:
        - "pwm_matrix" (dict): A dictionary with keys "A", "C", "G", "T" and values as probability lists.
        - "amount" (int): The amount of motifs to sample.

    Outputs:
        - "motif_strings" (list of str): A list of motif strings sampled based on the PWM probabilities.

    External Inputs:
        - "amount"
        
    Raises:
        - ValueError if the PWM format is invalid or the amount of samples is non-positive.
    """

    def __init__(self, amount=None):
        super().__init__(inputs=["pwm_matrix", "amount"], outputs=["motif_strings"], action=self.sample_motifs, external_inputs={"amount": amount})

    def sample_motifs(self, data):
        pwm_dict = data["pwm_matrix"]
        num_samples = data["amount"]

        if not isinstance(num_samples, int) or num_samples <= 0:
            raise ValueError("Amount of samples must be a positive integer.")

        for nt in ['A', 'C', 'G', 'T']:
            if nt not in pwm_dict:
                raise ValueError(f"Missing nucleotide '{nt}' in PWM.")

        motif_length = len(pwm_dict['A'])
        for nt in ['C', 'G', 'T']:
            if len(pwm_dict[nt]) != motif_length:
                raise ValueError("Inconsistent PWM row lengths.")

        nucleotides = ['A', 'C', 'G', 'T']
        motifs = []

        for _ in range(num_samples):
            motif = []
            for pos in range(motif_length):
                probs = [pwm_dict[nt][pos] for nt in nucleotides]
                chosen = random.choices(nucleotides, weights=probs, k=1)[0]
                motif.append(chosen)
            motifs.append(''.join(motif))

        return {"motif_strings": motifs}

    def __str__(self):
        """Returns a debug-friendly representation of the SampleMotifFromPWMPipe."""
        return f"SampleMotifFromPWMPipe(Sampling {self.outputs} based on PWM probabilities)"


class ValidateMotifStringPipe(FlowPipe):
    """
    FlowPipe to validate a motif string, ensuring it consists only of A, C, G, and T.

    Inputs:
        - "motif_string" (str): The motif string to validate.

    Outputs:
        - "motif_string" (str): The validated motif string.

    Raises:
        - ValueError if the motif contains invalid characters.
    """

    def __init__(self):
        super().__init__(inputs=["motif_string"], outputs=["motif_string"], action=self.validate_motif)

    def validate_motif(self, data):
        motif_str = data["motif_string"]
        allowed = set("ACGT")
        motif_str = motif_str.upper()
        for ch in motif_str:
            if ch not in allowed:
                raise ValueError(f"Invalid motif character '{ch}' in provided motif. Allowed characters are A, C, G, T.")
        return {"motif_string": motif_str}
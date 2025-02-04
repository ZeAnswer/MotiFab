import os
import random

# A small pseudocount to avoid zero (or extremely low) probabilities.
PSEUDOCOUNT = 0.0001

def generate_random_motif(length: int) -> str:
    """
    Generates a random motif of the specified length.
    
    Args:
        length (int): The length of the motif to generate.
        
    Returns:
        str: A randomly generated motif string composed of A, C, G, and T.
        
    Raises:
        ValueError: If length is not a positive integer.
    """
    if length <= 0:
        raise ValueError("Motif length must be a positive integer.")
    
    nucleotides = 'ACGT'
    return ''.join(random.choice(nucleotides) for _ in range(length))


def parse_pwm_file(file_path: str) -> dict:
    """
    Parses a PWM file in the expected format and returns a dictionary mapping
    nucleotides to lists of normalized probabilities.
    
    Expected PWM file format:
      - The first non-empty line must be: 
            #INCLUSive Motif Model
      - Followed by comment lines (starting with '#') that include (among other things):
            #W = <motif_width>
      - Then follows a block of lines containing the PWM matrix:
            Each line corresponds to one motif position and must contain four 
            tab- or space-separated decimal numbers representing Pr(A,i), Pr(C,i), 
            Pr(G,i), and Pr(T,i) for that position.
      - The block ends with a blank line.
      
    Processing:
      - Verifies the header and extracts the motif width from the "#W =" comment.
      - Reads the matrix lines. If exactly 4 rows are found, assumes a transposed format and transposes them.
      - Adds a pseudocount to each entry and normalizes each row.
      - Returns a dictionary with keys "A", "C", "G", and "T".
    
    Args:
        file_path (str): Path to the PWM file.
        
    Returns:
        dict: A dictionary with keys "A", "C", "G", "T" and values as lists of floats.
        
    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If required header information is missing or the PWM matrix is invalid.
    """
    try:
        with open(file_path, 'r') as f:
            raw_lines = f.readlines()
    except FileNotFoundError:
        raise FileNotFoundError(f"PWM file not found: {file_path}")
    
    # Remove blank lines and strip whitespace.
    lines = [line.strip() for line in raw_lines if line.strip() != '']
    
    # Verify the header line.
    if not lines or not lines[0].startswith("#INCLUSive Motif Model"):
        raise ValueError("PWM file does not start with the required header '#INCLUSive Motif Model'.")
    
    # Process comment lines to extract required information.
    motif_width = None
    index = 1  # Start after the header.
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
    
    # The rest of the lines should form the PWM matrix.
    matrix_lines = lines[index:]
    if not matrix_lines:
        raise ValueError("PWM file contains no matrix data.")
    
    # Parse each matrix line (expecting 4 numeric entries per line).
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
    
    # If there are exactly 4 rows, assume the matrix is transposed.
    if len(matrix) == 4:
        matrix = list(map(list, zip(*matrix)))
    
    if len(matrix) != motif_width:
        raise ValueError(f"Number of matrix rows ({len(matrix)}) does not match motif width ({motif_width}).")
    
    # Normalize each row after adding pseudocounts.
    normalized_matrix = []
    for row in matrix:
        new_row = [value + PSEUDOCOUNT for value in row]
        total = sum(new_row)
        if total == 0:
            raise ValueError("Row sums to zero after adding pseudocounts; cannot normalize.")
        normalized_row = [v / total for v in new_row]
        normalized_matrix.append(normalized_row)
    
    # Build the dictionary.
    pwm_dict = {"A": [], "C": [], "G": [], "T": []}
    for row in normalized_matrix:
        pwm_dict["A"].append(row[0])
        pwm_dict["C"].append(row[1])
        pwm_dict["G"].append(row[2])
        pwm_dict["T"].append(row[3])
    
    return pwm_dict


class PWM:
    """
    Represents a Position Weight Matrix (PWM) and provides a method to sample a motif.
    """
    def __init__(self, pwm_dict: dict):
        """
        Initializes the PWM with a dictionary mapping nucleotides to probability lists.
        
        Args:
            pwm_dict (dict): A dictionary with keys 'A', 'C', 'G', 'T' and values as lists of floats.
        
        Raises:
            ValueError: If required nucleotides are missing or if the lists have inconsistent lengths.
        """
        for nt in ['A', 'C', 'G', 'T']:
            if nt not in pwm_dict:
                raise ValueError(f"Missing nucleotide '{nt}' in PWM.")
        self.length = len(pwm_dict['A'])
        for nt in ['C', 'G', 'T']:
            if len(pwm_dict[nt]) != self.length:
                raise ValueError("Inconsistent PWM row lengths.")
        self.pwm = pwm_dict

    def sample(self) -> str:
        """
        Samples a motif string from the PWM.
        
        For each motif position, a nucleotide is chosen according to the column probabilities.
        
        Returns:
            str: A motif string sampled from the PWM.
        """
        motif = []
        nucleotides = ['A', 'C', 'G', 'T']
        for pos in range(self.length):
            probs = [self.pwm[nt][pos] for nt in nucleotides]
            chosen = random.choices(nucleotides, weights=probs, k=1)[0]
            motif.append(chosen)
        return ''.join(motif)


def validate_motif_string(motif_str: str) -> str:
    """
    Validates that the provided motif string contains only acceptable nucleotide characters.
    
    For our purposes, a valid motif must consist solely of the characters A, C, G, and T.
    
    Args:
        motif_str (str): The motif string to validate.
        
    Returns:
        str: The motif string converted to uppercase if valid.
        
    Raises:
        ValueError: If the motif contains invalid characters.
    """
    allowed = set("ACGT")
    motif_str = motif_str.upper()
    for ch in motif_str:
        if ch not in allowed:
            raise ValueError(f"Invalid motif character '{ch}' in provided motif. Allowed characters are A, C, G, T.")
    return motif_str


class Motif:
    """
    Encapsulates motif generation from a single input. The input is interpreted as follows:
    
      - If the input is an integer, it is taken as the motif length for random motif generation.
      - If the input is a string and input_type is "auto":
            • If the string corresponds to an existing file path, it is treated as a PWM file.
            • Otherwise, it is treated as a motif string.
      - The optional parameter 'input_type' may be explicitly set to "length", "string", or "file"
        to force one of the interpretations.
    
    In 'provided' or 'random' modes, the motif is generated or stored once during initialization.
    In 'pwm' mode, the PWM file is read once; each call to get_motif() samples a new motif.
    """
    def __init__(self, motif_input, input_type="auto"):
        """
        Initializes the Motif object.
        
        Args:
            motif_input: Either an integer (for random motif length) or a string.
            input_type (str): One of "auto" (default), "length", "string", or "file".
                              "auto" uses type and file-existence to decide.
        
        Raises:
            ValueError: If the input is ambiguous, invalid, or not in the correct motif format.
        """
        # Determine mode based on input_type.
        if input_type == "auto":
            if isinstance(motif_input, int):
                self.mode = "random"
            elif isinstance(motif_input, str):
                # If the string corresponds to an existing file, assume PWM mode.
                if os.path.isfile(motif_input):
                    self.mode = "pwm"
                else:
                    # Validate the motif string.
                    self.mode = "provided"
                    motif_input = validate_motif_string(motif_input)
            else:
                raise ValueError("Unsupported motif input type for 'auto'. Must be an int or str.")
        elif input_type == "length":
            if not isinstance(motif_input, int):
                raise ValueError("Input must be an integer when input_type is 'length'.")
            self.mode = "random"
        elif input_type == "string":
            if not isinstance(motif_input, str):
                raise ValueError("Input must be a string when input_type is 'string'.")
            self.mode = "provided"
            motif_input = validate_motif_string(motif_input)
        elif input_type == "file":
            if not isinstance(motif_input, str):
                raise ValueError("Input must be a string (file path) when input_type is 'file'.")
            if not os.path.isfile(motif_input):
                raise ValueError(f"Specified file does not exist: {motif_input}")
            self.mode = "pwm"
        else:
            raise ValueError("input_type must be one of 'auto', 'length', 'string', or 'file'.")

        # Set state based on the determined mode.
        if self.mode == "provided":
            self.motif = motif_input  # Use the validated motif string.
        elif self.mode == "random":
            # For random mode, motif_input should be an integer (length).
            self.motif = generate_random_motif(motif_input)
        elif self.mode == "pwm":
            # For PWM mode, motif_input is assumed to be a file path.
            pwm_dict = parse_pwm_file(motif_input)
            self.pwm_obj = PWM(pwm_dict)
        else:
            raise ValueError("Invalid motif mode.")

    def get_motif(self) -> str:
        """
        Returns a motif string based on the mode.
        
        - In 'provided' or 'random' modes, the motif is generated once upon initialization 
          and then returned every time.
        - In 'pwm' mode, a new motif is sampled from the stored PWM object on each call.
        
        Returns:
            str: The motif string.
        """
        if self.mode in ("provided", "random"):
            return self.motif
        elif self.mode == "pwm":
            return self.pwm_obj.sample()
        else:
            raise ValueError("Invalid motif mode.")


# For demonstration purposes:
if __name__ == "__main__":
    # Demonstrate provided motif mode (valid motif).
    try:
        motif1 = Motif("ACGTACGT", input_type="string")
        print("Provided motif:", motif1.get_motif())
    except ValueError as e:
        print("Error in provided motif:", e)

    # Demonstrate provided motif mode with an invalid motif.
    try:
        motif_invalid = Motif("helloWorld", input_type="auto")
        print("This should not print:", motif_invalid.get_motif())
    except ValueError as e:
        print("Error with invalid motif:", e)

    # Demonstrate random motif mode.
    motif2 = Motif(10, input_type="length")
    print("Random motif:", motif2.get_motif())

    # Demonstrate auto-detection:
    # If "example_pwm.txt" exists in the current directory, it will be interpreted as a PWM file;
    # otherwise, it will be treated as a motif string (and validated).
    motif3 = Motif("example_pwm.txt", input_type="auto")
    print("Auto-detected motif:", motif3.get_motif())

    # To test PWM mode explicitly, specify input_type="file" and supply a valid PWM file path.
    # Uncomment and update the file path as needed:
    # motif4 = Motif("path/to/your_pwm_file.txt", input_type="file")
    # print("PWM-sampled motif:", motif4.get_motif())
import random
from flowline import FlowPipe

class NaiveShufflePipe(FlowPipe):
    """
    FlowPipe to perform a naive nucleotide sequence shuffle on multiple sequences.

    Inputs:
        - "sequences" (list of str): The input nucleotide sequences.

    Outputs:
        - "sequences" (list of str): The shuffled sequences.

    Raises:
        - ValueError if the input is empty or contains invalid sequences.
    """

    def __init__(self):
        super().__init__(inputs=["sequences"], outputs=["sequences"], action=self.naive_shuffle)

    def naive_shuffle(self, data):
        sequences = data["sequences"]

        if not sequences:
            raise ValueError("Input sequences list cannot be empty.")

        shuffled_sequences = []
        for seq in sequences:
            if not seq:
                raise ValueError("A sequence in the list is empty.")

            seq_list = list(seq)
            random.shuffle(seq_list)
            shuffled_sequences.append(''.join(seq_list))

        return {"sequences": shuffled_sequences}

    def __str__(self):
        """Returns a debug-friendly representation of the NaiveShufflePipe."""
        return "NaiveShufflePipe(Shuffling each sequence in a list individually)"


class DiPairShufflePipe(FlowPipe):
    """
    FlowPipe to perform a dinucleotide-pair shuffle on multiple sequences.

    Inputs:
        - "sequences" (list of str): The input nucleotide sequences.

    Outputs:
        - "sequences" (list of str): The dinucleotide-pair shuffled sequences.

    Raises:
        - ValueError if the input is empty or contains invalid sequences.
    """

    def __init__(self):
        super().__init__(inputs=["sequences"], outputs=["sequences"], action=self.di_pair_shuffle)

    def di_pair_shuffle(self, data):
        sequences = data["sequences"]

        if not sequences:
            raise ValueError("Input sequences list cannot be empty.")

        shuffled_sequences = []
        for seq in sequences:
            if not seq:
                raise ValueError("A sequence in the list is empty.")

            # Partition the sequence into dinucleotide blocks.
            pairs = [seq[i:i+2] for i in range(0, len(seq) - 1, 2)]
            remainder = seq[-1] if len(seq) % 2 != 0 else None
            blocks = pairs + ([remainder] if remainder else [])

            random.shuffle(blocks)
            shuffled_sequences.append(''.join(blocks))

        return {"sequences": shuffled_sequences}

    def __str__(self):
        """Returns a debug-friendly representation of the DiPairShufflePipe."""
        return "DiPairShufflePipe(Shuffling each sequence in a list in dinucleotide pairs)"
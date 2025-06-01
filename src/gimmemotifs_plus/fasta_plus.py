import random
from gimmemotifs.fasta import Fasta


class FastaPlus(Fasta):
    """
    Extension of gimmemotifs.Fasta with built-in random sequence generation.
    """

    def _generate_random_sequence(
        self,
        min_length: int = 50,
        max_length: int | None = None,
        gc_content: float = 0.5
    ) -> str:
        """
        Generate a random DNA sequence with specified length and GC content.

        Args:
            min_length (int): Minimum length of the sequence (must be >0).
            max_length (int | None): Maximum length (>= min_length). If None, set to min_length.
            gc_content (float): Proportion of G/C between 0 and 1.

        Returns:
            str: Random DNA sequence.

        Raises:
            ValueError: On invalid parameter values.
        """
        # Validate lengths
        if not isinstance(min_length, int) or min_length <= 0:
            raise ValueError(f"min_length must be a positive integer, got {min_length}")
        if max_length is None:
            max_len = min_length
        else:
            if not isinstance(max_length, int) or max_length < min_length:
                raise ValueError(
                    f"max_length must be an integer >= min_length ({min_length}), got {max_length}"
                )
            max_len = max_length
        # Validate GC content
        if not isinstance(gc_content, (int, float)) or not (0 <= gc_content <= 1):
            raise ValueError(f"gc_content must be a float between 0 and 1, got {gc_content}")

        length = random.randint(min_length, max_len)
        # Compute weights for A, C, G, T
        weight_gc = gc_content / 2.0
        weight_at = (1.0 - gc_content) / 2.0
        # ensure no negative weights
        weights = [weight_at, weight_gc, weight_gc, weight_at]  # A, C, G, T

        nucleotides = "ACGT"
        return ''.join(random.choices(nucleotides, weights=weights, k=length))

    def populate_random_fasta(
        self,
        num_sequences: int = 10,
        min_length: int = 50,
        max_length: int | None = None,
        prefix: str = "seq",
        gc_content: float = 0.5
    ) -> None:
        """
        Populate this FastaPlus object with random sequences.

        Args:
            num_sequences (int): Number of sequences to add (must be >0).
            min_length (int): Minimum sequence length.
            max_length (int | None): Maximum sequence length.
            prefix (str): Prefix for sequence IDs.
            gc_content (float): Proportion of G/C between 0 and 1.

        Raises:
            ValueError: On invalid parameters.
        """
        if not isinstance(num_sequences, int) or num_sequences <= 0:
            raise ValueError(f"num_sequences must be a positive integer, got {num_sequences}")

        for i in range(1, num_sequences + 1):
            seq_str = self._generate_random_sequence(min_length, max_length, gc_content)
            seq_id = f"{prefix}{i}"
            self.add(seq_id, seq_str)
    
    def inject_motif(
        self,
        motif,
        injection_rate: float = None,
        injection_amount: int = None
    ) -> None:
        """
        Inject a motif into sequences in this FastaPlus object.

        Args:
            motif: Motif object to sample and inject.
            injection_rate (float): Fraction of sequences to inject (0 <= rate <= 1).
            injection_amount (int): Number of sequences to inject.
        Raises:
            ValueError: If neither injection_rate nor injection_amount is provided,
                        or if injection_rate is out of [0,1].
        """
        total = len(self)
        # Validate arguments
        if injection_rate is None and injection_amount is None:
            raise ValueError("Either injection_rate or injection_amount must be provided.")
        if injection_rate is not None:
            if not isinstance(injection_rate, (int, float)) or not (0.0 <= injection_rate <= 1.0):
                raise ValueError(f"injection_rate must be between 0 and 1, got {injection_rate}")
            count = int(round(injection_rate * total))
        else:
            count = injection_amount
            if not isinstance(count, int) or count < 0:
                raise ValueError(f"injection_amount must be a non-negative integer, got {injection_amount}")
        # Adjust amount if too large
        if count > total:
            print(f"Warning: injection_amount {count} > number of sequences {total}; injecting all sequences.")
            count = total
        if count == 0:
            return
        # Sample sequences indices to inject
        indices = random.sample(range(total), count)
        # Sample motif strings
        # motif.sample returns list of sequences
        motif_seqs = motif.sample(count)
        # Inject motifs
        for idx, mseq in zip(indices, motif_seqs):
            orig = self.seqs[idx]
            if len(orig) < len(mseq):
                continue
            pos = random.randint(0, len(orig) - len(mseq))
            new_seq = orig[:pos] + mseq + orig[pos + len(mseq):]
            # Update sequence
            self.seqs[idx] = new_seq

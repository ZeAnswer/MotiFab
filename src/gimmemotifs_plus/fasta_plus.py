import random
from gimmemotifs.fasta import Fasta
import logging
logger = logging.getLogger("gimmemotifs_plus.fasta_plus")

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

    def _get_exclusion_subset(self, excluded_fastaplus):
        """
        Get a subset of sequences that are not in the excluded FastaPlus object.
        if excluded_fastaplus is None, return all sequences.
        
        Args:
            excluded_fastaplus (FastaPlus): FastaPlus object with sequences to exclude.
        
        Returns:
            FastaPlus: New FastaPlus with sequences not in excluded_fastaplus.
        """
        if excluded_fastaplus is None:
            return self
        
        subset = FastaPlus()
        for seq_id, seq in self.items():
            if seq_id not in excluded_fastaplus.ids:
                subset.add(seq_id, seq)
        return subset

    def get_random(self, n, length=None):
        """Return n random sequences from this Fasta object"""
        random_fp = FastaPlus()
        if length:
            ids = self.ids[:]
            random.shuffle(ids)
            i = 0
            while (i < n) and (len(ids) > 0):
                seq_id = ids.pop()
                if len(self[seq_id]) >= length:
                    start = random.randint(0, len(self[seq_id]) - length)
                    random_fp[f"random{i + 1}"] = self[seq_id][start : start + length]
                    i += 1
            if len(random_fp) != n:
                logger.error("Not enough sequences of required length")
                return
            else:
                return random_fp
        else:
            print(f"@@@@@@@@@@ Selecting {n} random sequences from {len(self)} total sequences.")
            choice = random.sample(self.ids, n)
            for i in range(n):
                random_fp[choice[i]] = self[choice[i]]
        return random_fp

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

    def create_injected_subset(
        self,
        motif,
        amount,
        injection_rate: float = None,
        injection_amount: int = None
    ) -> "FastaPlus":
        """
        Create a new FastaPlus with injected motif sequences.
        Args:
            motif: MotifPlus object to inject.
            amount (int): Number of sequences in the new subset.
            injection_rate (float): Fraction of sequences to inject (0 <= rate <= 1).
            injection_amount (int): Number of sequences to inject.
        Returns:
            FastaPlus: New FastaPlus with injected sequences.
        """
        if not isinstance(amount, int) or amount <= 0:
            raise ValueError(f"amount must be a positive integer, got {amount}")
        # Create new FastaPlus
        new_fasta = self.get_random(amount)
        # Inject motif into the new subset
        new_fasta.inject_motif(motif, injection_rate, injection_amount)
        return new_fasta
    
    def create_non_overlapping_injected_subsets(
        self,
        motif,
        seq_amount,
        replicates: int = 1,
        background_length: int | None = None,
        injection_rate: float = None,
        injection_amount: int = None
    ) -> tuple[list["FastaPlus"], "FastaPlus | None"]:
        """
        Create multiple subsets with injected motifs. if background_length is provided,
        generate a background sequence of that length which is not overlapping
        with the other subsets.
        
        Args:
            motif: MotifPlus object to inject.
            seq_amount (int): Number of sequences in each subset.
            replicates (int): Number of subsets to create.
            background_length (int | None): Length of background sequence, if provided.
            injection_rate (float): Fraction of sequences to inject (0 <= rate <= 1).
            injection_amount (int): Number of sequences to inject.
        
        Returns:
            list[FastaPlus]: List of new FastaPlus objects with injected sequences.
            background (FastaPlus | None): Background FastaPlus object if background_length is provided, else None.
        """
        background = None
        if not isinstance(replicates, int) or replicates <= 0:
            raise ValueError(f"replicates must be a positive integer, got {replicates}")
        if not isinstance(seq_amount, int) or seq_amount <= 0:
            raise ValueError(f"seq_amount must be a positive integer, got {seq_amount}")
        # must have at least one of these
        if injection_rate is None and injection_amount is None:
            raise ValueError("Either injection_rate or injection_amount must be provided.")
        if injection_rate is not None:
            if not isinstance(injection_rate, (int, float)) or not (0.0 <= injection_rate <= 1.0):
                raise ValueError(f"injection_rate must be between 0 and 1, got {injection_rate}")
        else:
            if not isinstance(injection_amount, int) or injection_amount < 0:
                raise ValueError(f"injection_amount must be a non-negative integer, got {injection_amount}")
        # Create background if requested
        if background_length is not None and background_length > 0:
            background = self.get_random(background_length)
        # Create excluded subset if background is provided
        excluded_subset = self._get_exclusion_subset(background)
        subsets = []
        for i in range(replicates):
            # Create new subset
            new_subset = excluded_subset.create_injected_subset(
                motif,
                seq_amount,
                injection_rate=injection_rate,
                injection_amount=injection_amount
            )
            subsets.append(new_subset)
        return subsets, background
    
from gimmemotifs.motif.base import Motif


class MotifPlus(Motif):
    """
    Extends Motif to support initialization from consensus string (IUPAC) and
    optional mutation rate, in addition to pfm/ppm input.

    Parameters
    ----------
    pfm : array_like, optional
        Position frequency matrix (counts).
    ppm : array_like, optional
        Position probability matrix (fractions).
    consensus : str, optional
        Consensus sequence (IUPAC letters) to build motif.
    mutation_rate : float, optional
        If consensus uses simple A/C/G/T only, fraction (0-1) of mutations per position
        uniformly to other bases (default: 0.0).
    places : int, optional
        Decimal precision for rounding (default: 4).
    """
    def __init__(
        self,
        pfm=None,
        ppm=None,
        consensus=None,
        mutation_rate: float = 0.0,
        places: int = 4
    ):
        # If consensus provided, build ppm and delegate
        if consensus is not None:
            # Validate consensus
            if not isinstance(consensus, str) or len(consensus) == 0:
                raise ValueError(f"consensus must be a non-empty string, got {consensus!r}")
            # Validate mutation_rate
            if not isinstance(mutation_rate, (int, float)) or not (0.0 <= mutation_rate <= 1.0):
                raise ValueError(f"mutation_rate must be between 0.0 and 1.0, got {mutation_rate}")
            # Build position probability matrix (ppm)
            seq = consensus.upper()
            simple = all(b in "ACGT" for b in seq)
            built_ppm = []
            for char in seq:
                if simple and char in "ACGT":
                    share = mutation_rate / 3.0
                    probs = [share] * 4
                    idx = "ACGT".index(char)
                    probs[idx] = 1.0 - mutation_rate
                else:
                    # use IUPAC mapping or uniform fallback
                    probs = self.iupac_ppm.get(char, [0.25] * 4)
                built_ppm.append(probs)
            super().__init__(pfm=None, ppm=built_ppm, places=places)
            self.id = consensus
            self.consensus_str = consensus
            self.mutation_rate = mutation_rate
        else:
            # No consensus: use pfm/ppm as parent
            super().__init__(pfm=pfm, ppm=ppm, places=places)
            self.consensus_str = None
            self.mutation_rate = 0.0
    
    @classmethod
    def random_motif(
        cls,
        length: int,
        simple: bool = False
    ) -> "MotifPlus":
        """
        Generate a random motif of given length.
        If simple=True, build a pure consensus motif with random A/C/G/T per position (no mutations).
        Otherwise generate a random PPM for each position (values sum to 1).

        Args:
            length (int): Number of positions in the motif.
            simple (bool): If True, use a simple consensus-only motif.
        Returns:
            MotifPlus: Generated random motif.
        """
        import random
        # Simple consensus motif
        if simple:
            bases = "ACGT"
            consensus = ''.join(random.choice(bases) for _ in range(length))
            return cls(consensus=consensus, mutation_rate=0.0)
        # Random PPM-based motif
        ppm = []
        for _ in range(length):
            vals = [random.random() for _ in range(4)]
            total = sum(vals)
            # normalize to sum=1
            ppm.append([v / total for v in vals])
        return cls(pfm=None, ppm=ppm)


import random
from flowline import FlowPipe

class InjectMotifsIntoFastaRecordsPipe(FlowPipe):
    """
    FlowPipe to inject motifs into a subset of FASTA records.

    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
        - "motif_strings" (list): A list of motif strings to be injected.
        - "injection_rate" (int or str): Specifies how many records to inject motifs into.
          Can be:
            - An integer: Absolute number of records to inject.
            - A percentage string (e.g., "10%"): Percentage of total records to inject.

    Outputs:
        - "fasta_records" (list): The modified list of FASTA records with injected motifs.

    Raises:
        - ValueError if number is invalid.
        - ValueError if motif_strings is empty.
    """

    def __init__(self):
        super().__init__(
            inputs=["fasta_records", "motif_strings", "injection_rate"],
            outputs=["fasta_records"],
            action=self.inject_motifs
        )

    def inject_motifs(self, data):
        records = data["fasta_records"]
        motifs = data["motif_strings"]
        injection_rate = data["injection_rate"]

        if not motifs:
            raise ValueError("motif_strings cannot be empty.")

        num_records = len(records)

        # Determine how many records to inject into
        if isinstance(injection_rate, str) and injection_rate.endswith("%"):
            try:
                percentage = float(injection_rate.rstrip("%"))
                injection_count = int(round(num_records * percentage / 100))
            except Exception as e:
                raise ValueError(f"Invalid injection rate format: {injection_rate}") from e
        else:
            injection_count = int(injection_rate)

        injection_count = min(injection_count, num_records)

        # Randomly select records to inject motifs into
        injection_indices = random.sample(range(num_records), injection_count)

        # Perform round-robin selection of motifs
        new_records = []
        motif_index = 0

        for i, rec in enumerate(records):
            new_rec = rec.copy()
            if i in injection_indices:
                motif = motifs[motif_index]
                new_rec["seq"] = self.inject_motif(new_rec["seq"], motif)
                motif_index = (motif_index + 1) % len(motifs)  # Round-robin selection
            new_records.append(new_rec)

        return {"fasta_records": new_records}

    @staticmethod
    def inject_motif(seq, motif):
        """
        Injects the motif into the given sequence at a random position.
        If the sequence is shorter than the motif, returns the original sequence.
        """
        if len(seq) < len(motif):
            return seq
        pos = random.randint(0, len(seq) - len(motif))
        return seq[:pos] + motif + seq[pos + len(motif):]

    def __str__(self):
        """Returns a debug-friendly representation of the InjectMotifsIntoFastaRecordsPipe."""
        return "InjectMotifsIntoFastaRecordsPipe(Injecting motifs into a subset of FASTA records)"
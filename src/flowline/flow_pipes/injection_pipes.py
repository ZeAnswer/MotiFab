import random
from flowline import FlowPipe

class InjectMotifsIntoFastaRecordsPipe(FlowPipe):
    """
    FlowPipe to inject motifs into a subset of FASTA records.

    Inputs:
        - "fasta_records" (list): A list of FASTA record dictionaries.
        - "motif_strings" (list): A list of motif strings to be injected.
        - "amount" (int): The number of records to inject motifs into.

    Outputs:
        - "fasta_records" (list): The modified list of FASTA records with injected motifs.

    Raises:
        - ValueError if motif_strings is empty.
    """

    def __init__(self):
        super().__init__(
            inputs=["fasta_records", "motif_strings", "amount"],
            outputs=["fasta_records"],
            action=self.inject_motifs
        )

    def inject_motifs(self, data):
        records = data["fasta_records"]
        motifs = data["motif_strings"]
        injection_count = data["amount"]

        if not motifs:
            raise ValueError("motif_strings cannot be empty.")

        num_records = len(records)
        injection_count = min(injection_count, num_records)

        # Randomly select records to inject motifs into
        injection_indices = random.sample(range(num_records), injection_count)
        print ("@@@@@@@@@@@", "injection_indices", injection_indices)
        print ("@@@@@@@@@@@", "injection_count", injection_count)
        print ("@@@@@@@@@@@", "num_records", num_records)
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
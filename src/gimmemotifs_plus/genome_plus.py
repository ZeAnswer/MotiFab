"""
GenomePlus: resolve a genome identifier to a FASTA path.

This module accepts a string that is either:
 - a filesystem path to an existing FASTA (.fa/.fasta) file
 - a genome name known to genomepy

If the string is not a valid path, GenomePlus will check if that genome is installed;
if not, it will install it (including annotations) into a specified genomes directory.
After installation (or if already present), the genome FASTA path is returned.
"""
import os
from genomepy import list_installed_genomes, install_genome, Genome

#TODO make download not automatic but separated, or parameterized
class GenomePlus:
    def __init__(
        self,
        genome: str,
        genomes_dir: str = None,
        annotation: bool = True,
        threads: int = 1,
        force: bool = False
    ):
        """
        Parameters
        ----------
        genome : str
            Path to a FASTA file or a genomepy genome name.
        genomes_dir : str, optional
            Directory to install or look for genomepy genomes. If None, uses default.
        annotation : bool, default True
            Whether to download gene annotations along with the genome.
        threads : int, default 1
            Number of threads for genome installation.
        force : bool, default False
            If True, re-install genome even if already present.
        """
        self.genome = genome
        self.genomes_dir = genomes_dir
        self.annotation = annotation
        self.threads = threads
        self.force = force

    def resolve(self, install_missing_genome:bool = False) -> str:
        """
        Ensure the genome input is available as a FASTA file and return its path.

        Returns
        -------
        str
            Filesystem path to the genome FASTA.

        Raises
        ------
        RuntimeError
            If installation or resolution fails.
        """
        # If genome is a valid filesystem path, return it
        if os.path.exists(self.genome):
            print(f"Genome resolved from filesystem: {self.genome}")
            return os.path.abspath(self.genome)
        
        # if genome dir provided and doesn't exist, create it
        if self.genomes_dir and not os.path.exists(self.genomes_dir):
            print(f"Creating genomes directory: {self.genomes_dir}")
            os.makedirs(self.genomes_dir, exist_ok=True)

        # Otherwise treat as genomepy name
        name = self.genome
        # Check installed genomes
        installed = list_installed_genomes(self.genomes_dir)
        if name not in installed or self.force:
            if not install_missing_genome:
                raise RuntimeError(f"Genome '{name}' not found. If this is a file path, make sure it exists. If this is a genomepy genome name, ensure it is correct, installed or set install_genome=True (under genome_configurations) to install it automatically.")
            # Install genome (with annotations if requested)
            print(f"Genome '{name}' not found in installed genomes, installing...")
            try:
                install_genome(
                    name,
                    genomes_dir=self.genomes_dir,
                    annotation=self.annotation,
                    threads=self.threads,
                    force=self.force
                )
                print(f"Genome '{name}' installed successfully.")
            except Exception as e:
                print(f"Genome installation failed for '{name}': {e}")
                raise RuntimeError(f"Genome install failed for '{name}': {e}")

        # Use genomepy.Genome to get fasta path
        try:
            g = Genome(name, genomes_dir=self.genomes_dir)
            fasta_path = getattr(g, 'filename', None)
        except Exception as e:
            raise RuntimeError(f"Unable to load genome '{name}': {e}")

        if not fasta_path or not os.path.exists(fasta_path):
            raise RuntimeError(f"Genome FASTA not found for '{name}' at {fasta_path}")
        print(f"Genome resolved to FASTA path: {fasta_path}")
        return fasta_path

#example usage:
if __name__ == "__main__":
    genome_plus = GenomePlus("hg38", genomes_dir="/polio/oded/genomes")
    fasta_path = genome_plus.resolve()
    print(f"Resolved genome FASTA path: {fasta_path}")    
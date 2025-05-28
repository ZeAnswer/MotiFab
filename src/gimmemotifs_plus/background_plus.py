import sys
import logging
from gimmemotifs.background import create_background_file as _create_bg
from .fasta_plus import FastaPlus

logger = logging.getLogger("gimmemotifs_plus.background")


def create_background_file_plus(
    outfile,
    bg_type,
    fmt="fasta",
    size=None,
    genome=None,
    inputfile=None,
    number=10000,
    gc_content=0.5
):
    """
    Wrapper for create_background_file that adds a 'true_random' bg_type,
    generating sequences via FastaPlus.

    Parameters are the same as create_background_file, plus:
    - gc_content (float): GC proportion for 'true_random' backgrounds (0-1).
    """
    fmt = fmt.lower()
    if fmt in ["fa", "fsa"]:
        fmt = "fasta"

    valid_types = {"gc", "genomic", "random", "promoter", "true_random"}
    if bg_type not in valid_types:
        logger.error(
            f"The argument 'bg_type' must be one of: {', '.join(sorted(valid_types))}"
        )
        sys.exit(1)

    if bg_type == "true_random":
        # Only fasta is supported for true_random
        if fmt != "fasta":
            logger.error("true_random background can only be generated in FASTA format!")
            sys.exit(1)
        # Validate GC content
        if not isinstance(gc_content, (int, float)) or not (0 <= gc_content <= 1):
            logger.error(f"gc_content must be a float between 0 and 1, got {gc_content}")
            sys.exit(1)

        # Determine length bounds
        min_length = size if isinstance(size, int) and size > 0 else 50

        # Generate sequences
        fp = FastaPlus()
        fp.populate_random_fasta(
            num_sequences=number,
            min_length=min_length,
            prefix="random_seq",
            gc_content=gc_content,
        )
        # Write output
        try:
            fp.writefasta(outfile)
        except IOError as e:
            logger.error(f"Could not write true_random background to {outfile}: {e}")
            sys.exit(1)
    elif bg_type == "gc":
        # Allow generating a temp FASTA if no inputfile, based on gc_content
        if not inputfile:
            try:
                from tempfile import NamedTemporaryFile
                import os
            except ImportError:
                logger.error("Could not import tempfile for temp GC FASTA generation")
                sys.exit(1)
            # Determine sequence length for temp FASTA
            seq_len = size if isinstance(size, int) and size > 0 else 50
            # Create temp FASTA file
            tmp = NamedTemporaryFile(prefix="gc_bg_", suffix=".fa", delete=False)
            tmp_path = tmp.name
            tmp.close()
            # Populate random sequences at specified GC content
            fp = FastaPlus()
            fp.populate_random_fasta(
                num_sequences=number,
                min_length=seq_len,
                max_length=seq_len,
                prefix="gc_rand_",
                gc_content=gc_content,
            )
            fp.writefasta(tmp_path)
            # Delegate using temp file
            result = _create_bg(outfile, bg_type, fmt, size, genome, tmp_path, number)
            # Clean up temp FASTA
            try:
                os.remove(tmp_path)
            except Exception:
                logger.warning(f"Could not remove temp GC FASTA {tmp_path}")
            return result
        else:
            # Use provided inputfile
            return _create_bg(outfile, bg_type, fmt, size, genome, inputfile, number)
    else:
        # Delegate to original implementation for other types
        return _create_bg(outfile, bg_type, fmt, size, genome, inputfile, number)

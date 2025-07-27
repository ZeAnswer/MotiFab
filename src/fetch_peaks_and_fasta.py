import os
import gzip
import requests
import genomepy

# — Configuration —
GENOME = "hg38"
OUTDIR = "peak_fastas"
os.makedirs(OUTDIR, exist_ok=True)

# List your file accessions here:
accessions = [
    "ENCFF702XTL",  # GM12878 (from FilER)
    # "NGEN035720",  # HepG2
    # "NGEN035723",  # A549
]
files = {}
# Map accessions to file names
for acc in accessions:
    files[acc] = f"{acc}.bed.gz"  #TODO: Might want to expand this to include more file types, e.g. .narrowPeak.gz

BASE = "https://www.encodeproject.org/files" #TODO: might want to add more urls?
HEADERS = {"accept": "application/octet-stream"}

# Load genomepy Genome object
genome = genomepy.Genome(GENOME)

for acc, fname in files.items():
    # 1) Download compressed BED
    url = f"{BASE}/{acc}/@@download/{fname}"
    r = requests.get(url, headers=HEADERS)
    gz_path = os.path.join(OUTDIR, fname)
    with open(gz_path, "wb") as f:
        f.write(r.content)

    # 2) Uncompress to .bed
    bed_path = gz_path.rstrip(".gz")
    with gzip.open(gz_path, "rt") as inf, open(bed_path, "w") as outf:
        outf.write(inf.read())

    # 3) Convert BED → FASTA. the file name will be GENOME_<accession>.fa
    fasta_out = os.path.join(OUTDIR, f"{GENOME}_{acc}.fa")
    genome.track2fasta(bed_path, fastafile=fasta_out)

    print(f"Generated {fasta_out}")

    # 4) Clean up
    os.remove(gz_path)
    os.remove(bed_path)
print("All done!")

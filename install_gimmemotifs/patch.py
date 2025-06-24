import sys, os, shutil, tarfile, tempfile, subprocess
from urllib.request import urlopen

# 1. Download the tarball
URL = "https://github.com/vanheeringen-lab/gimmemotifs/releases/download/0.18.0/gimmemotifs-0.18.0.tar.gz"
print("Downloading gimmeMotifs v0.18.0…")
resp = urlopen(URL)
data = resp.read()

# 2. Write to temp and extract
tmp = tempfile.mkdtemp(prefix="gimme_install_")
tar_path = os.path.join(tmp, "gimmemotifs-0.18.0.tar.gz")
with open(tar_path, "wb") as f:
    f.write(data)
with tarfile.open(tar_path, "r:gz") as tar:
    tar.extractall(tmp)

# 3. Patch versioneer.py
gm_dir = os.path.join(tmp, "gimmemotifs-0.18.0")
ver_file = os.path.join(gm_dir, "versioneer.py")
print("Patching versioneer.py…")
for old, new in [
    ("SafeConfigParser", "ConfigParser"),
    ("readfp",        "read_file"),
]:
    with open(ver_file, "r") as f:
        text = f.read()
    text = text.replace(old, new)
    with open(ver_file, "w") as f:
        f.write(text)

# 4. Install
print("Installing…")
subprocess.check_call([sys.executable, "-m", "pip", "install", "--use-pep517", "--no-build-isolation", gm_dir])

# 5. Clean up
shutil.rmtree(tmp)
print("Done! gimmeMotifs should now be installed.")
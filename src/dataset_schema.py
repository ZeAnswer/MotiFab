import os
import json
from typing import Dict, List, Optional
from typing_extensions import Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import FieldValidationInfo

# Status states for runs
StatusType = Literal["pending", "generated", "running", "done", "skipped"]

class DenovoOut(BaseModel):
    denovo_out_dir: str = ""
    # Single PFM output
    gimme_denovo: str = ""
    # Stats per background types
    gimme_stats: Dict[str, str] = Field(default_factory=dict)
    # Combined motif file
    all_motifs: str = ""
    # Per-background motif stats
    all_motifs_stats: Dict[str, str] = Field(default_factory=dict)
    # Background types for initialization
    background_types: List[str] = Field(default_factory=list)
    @field_validator("gimme_denovo", "all_motifs")
    @classmethod
    def check_paths_exist(cls, v: str, info: FieldValidationInfo) -> str:
        if v and not os.path.exists(v):
            raise ValueError(f"{info.field_name} file not found: {v}")
        return v
    
    @model_validator(mode="after")
    def check_stats_paths(self) -> "DenovoOut":
        # validate gimme_stats entries
        for k, path in self.gimme_stats.items():
            if path and not os.path.exists(path):
                raise ValueError(f"gimme_stats[{k}] file not found: {path}")
        # validate all_motifs_stats entries
        for k, path in self.all_motifs_stats.items():
            if path and not os.path.exists(path):
                raise ValueError(f"all_motifs_stats[{k}] file not found: {path}")
        return self
    
    def validate(self) -> "DenovoOut":
        """Re-run validation on this model."""
        return self.model_validate(self.model_dump())
    
    def update_paths(
        self,
        denovo_out_dir: str,
        background_types: List[str]
    ) -> "DenovoOut":
        """
        Set output directory and auto-populate all related paths.
        """
        self.denovo_out_dir = denovo_out_dir
        self.background_types = background_types
        # Set single outputs
        self.gimme_denovo = os.path.join(denovo_out_dir, "gimme.denovo.pfm")
        # Stats per background
        self.gimme_stats = {
            bg: os.path.join(denovo_out_dir, f"stats.{bg}.txt")
            for bg in background_types
        }
        # All motifs
        self.all_motifs = os.path.join(denovo_out_dir, "intermediate", "all_motifs.pfm")
        # Per-background motif stats
        self.all_motifs_stats = {
            bg: os.path.join(denovo_out_dir, "intermediate", f"stats.{bg}.txt")
            for bg in background_types
        }
        # Validate paths
        return self.validate()

class ReplicateEntry(BaseModel):
    rep_dir: str = ""
    name: str = ""
    skip: bool = False
    test_fa: str = ""
    status: StatusType = "pending"
    denovo_out: DenovoOut = Field(default_factory=DenovoOut)

    @field_validator("test_fa")
    @classmethod
    def check_test_fa_exists(cls, v: str, info: FieldValidationInfo) -> str:
        if v and not os.path.exists(v):
            raise ValueError(f"test_fa not found: {v}")
        return v
    
    def validate(self) -> "ReplicateEntry":
        """Re-run validation on this model."""
        return self.model_validate(self.model_dump())

class CombinationEntry(BaseModel):
    comb_dir: str = ""
    name: str = ""
    skip: bool = False
    seq_amount: int = 0
    injection_rate: float = 0.0
    n_replicates: int = 0
    custom_bg: str = ""
    status: StatusType = "pending"
    replicates: Dict[str, ReplicateEntry] = Field(default_factory=dict)

    @model_validator(mode="after")
    def populate_replicates(self) -> "CombinationEntry":
        """
        Auto-generate replicate entries if none provided, based on n_replicates.
        """
        if not self.replicates and self.n_replicates > 0:
            self.replicates = {
                f"rep_{i}": ReplicateEntry(name=f"{self.name}_rep_{i}")
                for i in range(1, self.n_replicates + 1)
            }
        return self
    
    @field_validator("custom_bg")
    @classmethod
    def check_custom_bg_exists(cls, v: str, info: FieldValidationInfo) -> str:
        if v and not os.path.exists(v):
            raise ValueError(f"custom_bg file not found: {v}")
        return v
    
    def validate(self) -> "CombinationEntry":
        """Re-run validation on this model."""
        return self.model_validate(self.model_dump())


class DatasetConfig(BaseModel):
    master_fasta: str = ""
    genome_fasta: str = ""
    injected_motif: str = ""
    output_dir: str = ""
    seq_amounts: List[int] = Field(default_factory=list)
    injection_rates: List[float] = Field(default_factory=list)
    n_replicates: int = 0
    background_types: List[str] = Field(default_factory=list)
    combinations: Dict[str, CombinationEntry] = Field(default_factory=dict)

    @model_validator(mode="after")
    def populate_combinations(self) -> "DatasetConfig":
        """
        Auto-build combination entries for any seq_amount/rate pair not already in combinations.
        """
        if self.seq_amounts and self.injection_rates and self.n_replicates > 0:
            for seq_amount in self.seq_amounts:
                for injection_rate in self.injection_rates:
                    key = f"len_{seq_amount}_rate_{int(injection_rate * 100)}"
                    if key not in self.combinations:
                        self.combinations[key] = CombinationEntry(
                            name=key,
                            seq_amount=seq_amount,
                            injection_rate=injection_rate,
                            n_replicates=self.n_replicates,
                        )
        return self
    
    @field_validator("master_fasta", "genome_fasta", "injected_motif")
    @classmethod
    def check_config_paths(cls, v: str, info: FieldValidationInfo) -> str:
        if v and not os.path.exists(v):
            raise ValueError(f"{info.field_name} not found: {v}")
        return v
    
    def validate(self) -> "DatasetConfig":
        """Re-run validation on this model."""
        return self.model_validate(self.model_dump())

    def save(self, path: str) -> None:
        """Save configuration to JSON."""
        # Use model_dump_json (Pydantic V2) instead of deprecated json()
        with open(path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, path: str) -> "DatasetConfig":
        """Load configuration from JSON, reconstructing nested models."""
        # Read file and validate JSON into model (Pydantic V2)
        with open(path, "r") as f:
            raw = f.read()
        return cls.model_validate_json(raw)


if __name__ == "__main__":
    # Example usage
    config = DatasetConfig(
        seq_amounts=[100, 200],
        injection_rates=[0.1, 0.2],
        n_replicates=3,
        background_types=["custom", "random"]
    )
    
    # Validate and save
    config.validate()
    config.save("dataset_config.json")
    
    # Load back
    loaded_config = DatasetConfig.load("dataset_config.json")
    print(loaded_config)
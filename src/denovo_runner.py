from dataset_manager import DatasetManager
import os
import json
from gimmemotifs.motif.denovo import gimme_motifs
from typing import List, Optional
class DenovoRunner:
    def __init__(self, dataset_manager: DatasetManager):
        self.dataset_manager = dataset_manager
        self.params = dataset_manager.get_denovo_params()
    
    def _update_replicate_result_paths(
        self,
        replicate: dict):
        """Update paths in the replicate entry based on the denovo output directory."""
        if not replicate.get('gimme_out_dir'):
            raise ValueError("gimme_out_dir is required in the replicate entry")
        gimme_out_dir = replicate['gimme_out_dir']
        # Update paths for gimme_denovo and stats files
        replicate['gimme_denovo'] = os.path.join(gimme_out_dir, 'gimme.denovo.pfm')
        replicate['gimme_stats'] = {
            bg_type: os.path.join(gimme_out_dir, f'stats.{bg_type}.txt')
            for bg_type in self.params.get('background_types', [])
        }
        # Update all_motifs and all_motifs_stats paths
        replicate['all_motifs'] = os.path.join(gimme_out_dir, 'intermediate', 'all_motifs.pfm')
        replicate['all_motifs_stats'] = {
            bg_type: os.path.join(gimme_out_dir, 'intermediate', f'stats.{bg_type}.txt')
            for bg_type in self.params.get('background_types', [])
        }
        # replicate['images_dir'] = os.path.join(gimme_out_dir, 'images')
        return replicate

    def _run_denovo_on_single_replicate(
        self,
        replicate: dict,
        run_params: dict,):
        """a replicate is in the format:
            "name": string
            "dir": string
            'gimme_out_dir': string
            "test_fasta": string
            "background_fasta": string or None
            "status": string
        """
        # Validate replicate entry
        if not replicate.get('name'):
            raise ValueError("Replicate name is required")
        test_fasta = replicate.get('test_fasta')
        if not test_fasta or not os.path.exists(test_fasta):
            raise ValueError(f"Test FASTA file not found for replicate {replicate['name']}: {test_fasta}")
        gimme_out_dir = replicate.get('gimme_out_dir')
        if not gimme_out_dir:
            raise ValueError(f"gimme_out_dir not specified for replicate {replicate['name']}")
        os.makedirs(gimme_out_dir, exist_ok=True)
        background_fasta = replicate.get('background_fasta')
        if background_fasta and not os.path.exists(background_fasta):
            raise ValueError(f"Background FASTA file not found for replicate {replicate['name']}: {background_fasta}")
        
        # Prepare run parameters
        replicate_run_params = run_params.copy()
        if run_params.get('background') and 'custom' in run_params['background'] and background_fasta:
            replicate_run_params['custom_background'] = background_fasta
            
        # Run gimme_motifs for this replicate
        try:
            results = gimme_motifs(
                inputfile=test_fasta,
                outdir=gimme_out_dir,
                params=replicate_run_params
            )
            # Update replicate status
            replicate['status'] = 'completed_denovo'
            replicate = self._update_replicate_result_paths(replicate)
            
            print(f"Replicate {replicate['name']} completed successfully. Results saved to {gimme_out_dir}.")
            return replicate
        except Exception as e:
            replicate['status'] = 'failed_denovo'
            replicate['error'] = str(e)
            # update the replicate entry in the dataset manager
            print(f"Replicate {replicate['name']} failed: {e}.")
            return replicate


    def _denovo_parallel_runner(self, replicates, run_params, max_parallel=5, delay=1000):
        from concurrent.futures import ProcessPoolExecutor, as_completed
        from time import sleep as time_sleep
        # space out submissions
        futures = {}
        with ProcessPoolExecutor(max_workers=max_parallel) as exe:
            for i, rep in enumerate(replicates):
                futures[exe.submit(self._run_denovo_on_single_replicate, rep, run_params)] = rep
                if i < len(replicates)-1:
                    time_sleep(delay/1000)

            for fut in as_completed(futures):
                rep = futures[fut]
                try:
                    new_rep = fut.result()
                    self.dataset_manager.upsert_rep_by_name(rep['name'], new_rep)
                except Exception as e:
                    print(f"Replicate {rep['name']} failed: {e}")

    def run_denovo(
        self,
        genome_fasta: Optional[str] = None,
        background_types: Optional[List[str]] = None,
        ncpus: Optional[int] = 10,
        tools: Optional[List[str]] = ["BioProspector", "MEME", "Homer"],
        max_parallel: Optional[int] = 5,
        rerun_failed: bool = False,
        force: bool = False
    ):
        """
        Generate datasets based on configuration and provided parameters.
        Parameters overwrite JSON values if given.
        """
        # Merge provided parameters into nested config
        params = self.params
        for key, val in [
            ('genome_fasta', genome_fasta),
            ('background_types', background_types),
            ('ncpus', ncpus),
            ('tools', tools),
            ('max_parallel', max_parallel),
            ('rerun_failed', rerun_failed),
            ('force', force)
        ]:
            if val is not None:
                params[key] = val

        genome = params.get('genome_fasta')
        if genome and not os.path.exists(genome):
            raise FileNotFoundError(f"genome_fasta not found: {genome}")

        # Required parameters
        # required denovo params
        for param in ['background_types', 'ncpus', 'tools', 'max_parallel']:
            val = params.get(param)
            if not val:
                raise ValueError(f"{param} must be provided via configuration or parameter")
        
    #if we pass validations, need to update the config with these values
        self.dataset_manager.update_denovo_params(params)
        #TODO: this might need to be handled differently since we are overwriting the config

        # background types must not be empty and all values must be within the allowed set:
        # 'random'
        # 'genomic' - requires genome_fasta
        # 'gc' - requires genome_fasta
        # 'custom' - requires backgrounds to be provided. will be validated later
        allowed_bg_types = {'random', 'genomic', 'gc', 'custom'}
        bg_types = params.get('background_types', [])
        if not bg_types or not all(t in allowed_bg_types for t in bg_types):
            raise ValueError(f"background_types must be at least one of: {', '.join(sorted(allowed_bg_types))} (string list)")
        if 'genomic' in bg_types and not genome:
            raise ValueError("genomic background type requires genome_fasta to be provided")
        if 'gc' in bg_types and not genome:
            raise ValueError("gc background type requires genome_fasta to be provided")

        # validate tools
        allowed_tools = {'AMD', 'BioProspector', 'ChIPMunk', 'DiNAMO', 'GADEM', 'HMS', 'Homer', 'Improbizer', 'MDmodule', 'MEME', 'MEMEW', 'MotifSampler', 'Posmo', 'ProSampler', 'Trawler', 'Weeder', 'XXmotif'}
        tools = params.get('tools', [])
        if not tools or not all(t in allowed_tools for t in tools):
            raise ValueError(f"tools must be at least one of: {', '.join(sorted(allowed_tools))} (string list)")

        # Prepare run parameters
        run_params = {
            'genome': genome,
            'background': ",".join(bg_types),
            'ncpus': ncpus,
            'tools': ",".join(tools),
            'denovo': True,
            'keep_intermediate': True,
        }
        
        # determine which replicates to run
        force_all = params.get('force', False)
        rerun_failed_flag = params.get('rerun_failed', False)
        # Get all replicates from the dataset manager
        replicates = self.dataset_manager.get_all_reps()
        if not replicates:
            raise ValueError("No replicates found in the dataset manager")
        # only include reps that need running
        statuses = ['generated']
        if rerun_failed_flag:
            statuses.append('failed_denovo')
        filtered = []
        for rep in replicates:
            if force_all or rep.get('force', False):
                filtered.append(rep)
            elif rep.get('status') in statuses:
                filtered.append(rep)
        replicates = filtered
        if not replicates:
            raise ValueError("No replicates to run denovo")

        # Run denovo motif discovery in parallel
        self._denovo_parallel_runner(
            replicates,
            run_params,
            max_parallel=params.get('max_parallel', 5),
            delay=1000
        )

   # Example usage:
if __name__ == "__main__":
    manager = DatasetManager('/polio/oded/MotiFabEnv/presentation_run/dataset_config.json')
    runner = DenovoRunner(manager)
    try:
        runner.run_denovo(
        )
    except Exception as e:
        print(f"Error running denovo: {e}")
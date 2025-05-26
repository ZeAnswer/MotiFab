#!/usr/bin/env python3
"""
Flow pipes for motif enrichment analysis using tools like MEME and HOMER.
These pipes generate commands, submit jobs to cluster systems, and collect results.
#TODO: Change some input and output names
"""
import os
import re
import time
import subprocess
from flowline import FlowPipe

class DeNovoCommandGeneratorPipe(FlowPipe):
    """
    Parent class for de novo motif discovery command generators.
    
    Input:
        summary_record:
            - test_fasta_path: Path to the test FASTA file
            - background_fasta_path: Path to the background FASTA file
            - run_id: Identifier for this test run
        
    Output:
        - command: The command to run
        - output_dir: Complete output directory path (prefix + run_id)
        - run_id: The test identifier (passed through)
    """
    
    def __init__(self, output_dir_prefix="output", files_dir=None, extra_params=None, 
                 motif_length=None, num_motifs=None, strand=None, revcomp=None):
        """
        Initialize the de novo command generator pipe.
        
        Args:
            output_dir_prefix: Prefix for output directories
            files_dir: Directory where input files are stored
            extra_params: Additional parameters to append to the command
            motif_length: Motif length specification (e.g., "5-8" for range or "5,6,7,8" for list)
            num_motifs: Number of motifs to find
            strand: Strand specification ("+", "-", "both")
            revcomp: Whether to consider reverse complement (True/False)
        """
        # Define input and output names
        inputs = ["summary_record"]
        outputs = ["command", "output_dir", "run_id"]
        
        # Call parent constructor with our inputs, outputs, and action
        super().__init__(inputs=inputs, outputs=outputs, action=self._generate_command)
        
        # Store parameters
        self.output_dir_prefix = output_dir_prefix
        self.files_dir = files_dir
        self.extra_params = extra_params or ""
        
        # Store common parameters
        self.motif_length = motif_length
        self.num_motifs = num_motifs
        self.strand = strand
        self.revcomp = revcomp
    
    def _validate_inputs(self, summary_record):
        """Validate required inputs."""
        if not summary_record:
            raise ValueError("Missing required input: summary_record")
        if not isinstance(summary_record, dict):
            raise ValueError("summary_record must be a dictionary")
            
        test_fasta_path = summary_record.get("test_fasta_path")
        background_fasta_path = summary_record.get("background_fasta_path")
        run_id = summary_record.get("run_id")
        
        # Validate required inputs
        if not test_fasta_path:
            raise ValueError("Missing required input: test_fasta_path")
        if not background_fasta_path:
            raise ValueError("Missing required input: background_fasta_path")
        if not run_id:
            raise ValueError("Missing required input: run_id")
            
        return test_fasta_path, background_fasta_path, run_id
    
    def _prepare_paths(self, test_fasta_path, background_fasta_path, run_id):
        """Prepare file paths and create output directory."""
        # Create the complete output directory path
        output_dir = os.path.join(self.output_dir_prefix, run_id)
        
        # Adjust paths if files_dir is provided
        test_fasta_path = os.path.join(self.files_dir, test_fasta_path) if self.files_dir else test_fasta_path
        background_fasta_path = os.path.join(self.files_dir, background_fasta_path) if self.files_dir else background_fasta_path
        
        # Validate file paths
        if not os.path.exists(test_fasta_path):
            raise ValueError(f"Test FASTA file not found: {test_fasta_path}")
        if not os.path.exists(background_fasta_path):
            raise ValueError(f"Background FASTA file not found: {background_fasta_path}")
            
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        return test_fasta_path, background_fasta_path, output_dir
    
    def _parse_motif_length(self, motif_length_str):
        """
        Parse motif length specification into a format suitable for the tool.
        
        Args:
            motif_length_str: String specification of motif length (e.g., "5-8" or "5,6,7,8")
            
        Returns:
            Tool-specific representation of motif length
        """
        if not motif_length_str:
            return None
            
        # Check if it's a range (e.g., "5-8")
        range_match = re.match(r'^(\d+)-(\d+)$', motif_length_str)
        if range_match:
            min_length = int(range_match.group(1))
            max_length = int(range_match.group(2))
            return self._convert_range_to_tool_format(min_length, max_length)
            
        # Check if it's a comma-separated list (e.g., "5,6,7,8")
        list_match = re.match(r'^\d+(,\d+)*$', motif_length_str)
        if list_match:
            lengths = [int(x) for x in motif_length_str.split(',')]
            return self._convert_list_to_tool_format(lengths)
            
        # If it's a single number
        single_match = re.match(r'^\d+$', motif_length_str)
        if single_match:
            length = int(motif_length_str)
            return self._convert_single_to_tool_format(length)
            
        raise ValueError(f"Invalid motif length specification: {motif_length_str}")
    
    def _convert_range_to_tool_format(self, min_length, max_length):
        """
        Convert a range of motif lengths to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_range_to_tool_format")
    
    def _convert_list_to_tool_format(self, lengths):
        """
        Convert a list of motif lengths to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_list_to_tool_format")
    
    def _convert_single_to_tool_format(self, length):
        """
        Convert a single motif length to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_single_to_tool_format")
    
    def _parse_strand(self, strand_str):
        """
        Parse strand specification into a format suitable for the tool.
        
        Args:
            strand_str: String specification of strand ("+", "-", "both")
            
        Returns:
            Tool-specific representation of strand
        """
        if not strand_str:
            return None
            
        strand_str = strand_str.lower()
        if strand_str in ["+", "plus", "forward"]:
            return self._convert_forward_strand_to_tool_format()
        elif strand_str in ["-", "minus", "reverse"]:
            return self._convert_reverse_strand_to_tool_format()
        elif strand_str in ["both", "b"]:
            return self._convert_both_strand_to_tool_format()
        else:
            raise ValueError(f"Invalid strand specification: {strand_str}")
    
    def _convert_forward_strand_to_tool_format(self):
        """
        Convert forward strand specification to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_forward_strand_to_tool_format")
    
    def _convert_reverse_strand_to_tool_format(self):
        """
        Convert reverse strand specification to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_reverse_strand_to_tool_format")
    
    def _convert_both_strand_to_tool_format(self):
        """
        Convert both strand specification to tool-specific format.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _convert_both_strand_to_tool_format")
    
    def _generate_command(self, data):
        """
        Generate command based on input parameters.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _generate_command")

class MemeCommandGeneratorPipe(DeNovoCommandGeneratorPipe):
    """
    Pipe for generating MEME commands based on input parameters.
    
    Input:
        summary_record:
            - test_fasta_path: Path to the test FASTA file
            - background_fasta_path: Path to the background FASTA file
            - run_id: Identifier for this test run
        
    Output:
        - command: The MEME command to run
        - output_dir: Complete output directory path (prefix + run_id)
        - run_id: The test identifier (passed through)
    """
    
    def __init__(self, meme_params=None, output_dir_prefix="meme_output", files_dir=None, extra_params=None,
                 motif_length=None, num_motifs=None, strand=None, revcomp=None):
        """
        Initialize the MEME command generator pipe.
        
        Args:
            meme_params: MEME parameters to use for command generation
            output_dir_prefix: Prefix for output directories
            files_dir: Directory where input files are stored
            extra_params: Additional parameters to append to the command
            motif_length: Motif length specification (e.g., "5-8" for range)
            num_motifs: Number of motifs to find
            strand: Strand specification ("+", "-", "both")
            revcomp: Whether to consider reverse complement (True/False)
        """
        # Call parent constructor
        super().__init__(output_dir_prefix=output_dir_prefix, files_dir=files_dir, extra_params=extra_params,
                         motif_length=motif_length, num_motifs=num_motifs, strand=strand, revcomp=revcomp)
        
        # Store MEME parameters
        self.meme_params = meme_params or {}
        
        # Update meme_params with common parameters if provided
        if motif_length:
            motif_length_params = self._parse_motif_length(motif_length)
            if motif_length_params:
                self.meme_params.update(motif_length_params)
                
        if num_motifs:
            self.meme_params["nmotifs"] = num_motifs
            
        if strand:
            strand_param = self._parse_strand(strand)
            if strand_param:
                self.meme_params.update(strand_param)
                
        if revcomp is not None:
            self.meme_params["revcomp"] = revcomp
    
    def _convert_range_to_tool_format(self, min_length, max_length):
        """Convert a range of motif lengths to MEME format."""
        return {
            "minw": min_length,
            "maxw": max_length
        }
    
    def _convert_list_to_tool_format(self, lengths):
        """Convert a list of motif lengths to MEME format."""
        # MEME doesn't support a list of lengths directly, so we'll use the min and max
        return {
            "minw": min(lengths),
            "maxw": max(lengths)
        }
    
    def _convert_single_to_tool_format(self, length):
        """Convert a single motif length to MEME format."""
        return {
            "minw": length,
            "maxw": length
        }
    
    def _convert_forward_strand_to_tool_format(self):
        """Convert forward strand specification to MEME format."""
        return {
            "revcomp": False
        }
    
    def _convert_reverse_strand_to_tool_format(self):
        """Convert reverse strand specification to MEME format."""
        return {
            "revcomp": False
        }
    
    def _convert_both_strand_to_tool_format(self):
        """Convert both strand specification to MEME format."""
        return {
            "revcomp": True
        }
    
    def _generate_command(self, data):
        """Generate MEME command based on input parameters."""
        # Extract and validate required inputs
        summary_record = data.get("summary_record")
        test_fasta_path, background_fasta_path, run_id = self._validate_inputs(summary_record)
        
        # Prepare paths and create output directory
        test_fasta_path, background_fasta_path, output_dir = self._prepare_paths(
            test_fasta_path, background_fasta_path, run_id
        )
        
        # Build the MEME command
        cmd_parts = ["meme"]
        
        # Add the test FASTA file
        cmd_parts.append(f'"{test_fasta_path}"')
        
        # Add boolean flags
        if self.meme_params.get('dna', True):
            cmd_parts.append("-dna")
        if self.meme_params.get('revcomp', True):
            cmd_parts.append("-revcomp")
            
        # Add parameters with values
        for param, value in self.meme_params.items():
            if param in ['dna', 'revcomp']:  # Already handled as boolean flags
                continue
                
            # Format parameters with hyphens
            param_str = param.replace('_', '-')
            
            # Add the parameter
            if value is not None:
                cmd_parts.append(f"-{param_str} {value}")
        
        # Add the background file
        cmd_parts.append(f'-neg "{background_fasta_path}"')
        
        # Add the output directory
        cmd_parts.append(f'-oc "{output_dir}"')
        
        # Add any extra parameters
        if self.extra_params:
            cmd_parts.append(self.extra_params)
        
        # Construct the final command
        command = " ".join(cmd_parts)
        
        # Return the results
        return {
            "command": command,
            "output_dir": output_dir,
            "run_id": run_id
        }

# class HomerCommandGeneratorPipe(DeNovoCommandGeneratorPipe):
#     """
#     Pipe for generating HOMER commands based on input parameters.
    
#     Input:
#         summary_record:
#             - test_fasta_path: Path to the test FASTA file
#             - background_fasta_path: Path to the background FASTA file
#             - run_id: Identifier for this test run
        
#     Output:
#         - command: The HOMER command to run
#         - output_dir: Complete output directory path (prefix + run_id)
#         - run_id: The test identifier (passed through)
#     """
    
#     def __init__(self, homer_params=None, output_dir_prefix="homer_output", files_dir=None, extra_params=None,
#                  motif_length=None, num_motifs=None, strand=None, revcomp=None):
#         """
#         Initialize the HOMER command generator pipe.
        
#         Args:
#             homer_params: HOMER parameters to use for command generation
#             output_dir_prefix: Prefix for output directories
#             files_dir: Directory where input files are stored
#             extra_params: Additional parameters to append to the command
#             motif_length: Motif length specification (e.g., "5,6,7,8" for list)
#             num_motifs: Number of motifs to find
#             strand: Strand specification ("+", "-", "both")
#             revcomp: Whether to consider reverse complement (True/False)
#         """
#         # Call parent constructor
#         super().__init__(output_dir_prefix=output_dir_prefix, files_dir=files_dir, extra_params=extra_params,
#                          motif_length=motif_length, num_motifs=num_motifs, strand=strand, revcomp=revcomp)
        
#         # Store HOMER parameters
#         self.homer_params = homer_params or {}
        
#         # Update homer_params with common parameters if provided
#         if motif_length:
#             motif_length_params = self._parse_motif_length(motif_length)
#             if motif_length_params:
#                 self.homer_params.update(motif_length_params)
                
#         if num_motifs:
#             self.homer_params["S"] = num_motifs
            
#         if strand:
#             strand_param = self._parse_strand(strand)
#             if strand_param:
#                 self.homer_params.update(strand_param)
                
#         if revcomp is not None:
#             # HOMER doesn't have a direct revcomp parameter, it's handled by strand
#             pass
    
#     def _convert_range_to_tool_format(self, min_length, max_length):
#         """Convert a range of motif lengths to HOMER format."""
#         # HOMER doesn't support a range directly, so we'll generate a comma-separated list of lengths
#         return {
#             "len": ",".join(str(x) for x in range(min_length, max_length + 1))
#         }
    
#     def _convert_list_to_tool_format(self, lengths):
#         """Convert a list of motif lengths to HOMER format."""
#         return {
#             "len": ",".join(str(x) for x in lengths)
#         }
    
#     def _convert_single_to_tool_format(self, length):
#         """Convert a single motif length to HOMER format."""
#         return {
#             "len": length
#         }
    
#     def _convert_forward_strand_to_tool_format(self):
#         """Convert forward strand specification to HOMER format."""
#         return {
#             "strand": "+"
#         }
    
#     def _convert_reverse_strand_to_tool_format(self):
#         """Convert reverse strand specification to HOMER format."""
#         return {
#             "strand": "-"
#         }
    
#     def _convert_both_strand_to_tool_format(self):
#         """Convert both strand specification to HOMER format."""
#         return {
#             "strand": "both"
#         }
    
#     def _generate_command(self, data):
#         """Generate HOMER command based on input parameters."""
#         # Extract and validate required inputs
#         summary_record = data.get("summary_record")
#         test_fasta_path, background_fasta_path, run_id = self._validate_inputs(summary_record)
        
#         # Prepare paths and create output directory
#         test_fasta_path, background_fasta_path, output_dir = self._prepare_paths(
#             test_fasta_path, background_fasta_path, run_id
#         )
        
#         # Build the HOMER command
#         cmd_parts = ["homer2 denovo"]
        
#         # Add the input and background files
#         cmd_parts.append(f'-i "{test_fasta_path}"')
#         cmd_parts.append(f'-b "{background_fasta_path}"')
        
#         # Add the output file
#         output_file = os.path.join(output_dir, "homer.txt")
#         #cmd_parts.append(f'-o "{output_file}"')
        
#         # Add parameters with values
#         for param, value in self.homer_params.items():
#             # Format parameters with hyphens
#             param_str = param.replace('_', '-')
            
#             # Add the parameter
#             if value is not None:
#                 cmd_parts.append(f"-{param_str} {value}")
        
#         # Add any extra parameters
#         if self.extra_params:
#             cmd_parts.append(self.extra_params)
        
#         # Construct the final command
#         command = " ".join(cmd_parts)
        
#         # Return the results
#         return {
#             "command": command,
#             "output_dir": output_dir,
#             "run_id": run_id
#         }

# class SlurmJobGeneratorPipe(FlowPipe):
#     """
#     Pipe for generating SLURM job submission scripts.
    
#     Input:
#         - command: Command to run in the SLURM job
#         - output_dir: Directory to store job outputs
        
#     Output:
#         - job_script: Path to the generated job script #TODO change this to job path
#     """
    
#     def __init__(self, job_name="motif_analysis", slurm_params=None, module_name=None):
#         """
#         Initialize the SLURM job generator pipe.
        
#         Args:
#             job_name: Name for the SLURM job
#             slurm_params: SLURM parameters (time, mem, cpus, etc.)
#             module_name: Module to load before running the command (e.g. meme-5.4.1)
#         """
#         # Define input and output names
#         inputs = ["command", "output_dir"]
#         outputs = ["job_script"]
        
#         # Call parent constructor with our inputs, outputs, and action
#         super().__init__(inputs=inputs, outputs=outputs, action=self._generate_job_script)
        
#         # Store parameters
#         self.job_name = job_name
#         self.module_name = module_name
        
#         # Default SLURM parameters
#         default_params = {
#             "time": "4:00:00",
#             "mem": "16GB",
#             "cpus_per_task": 4,
#             "partition": None
#         }
        
#         # Update with user-provided parameters if any
#         self.slurm_params = default_params
#         if slurm_params:
#             self.slurm_params.update(slurm_params)
    
#     def _generate_job_script(self, data):
#         """Generate a SLURM job script based on the provided command and output directory."""
#         # Extract required inputs
#         command = data.get("command")
#         output_dir = data.get("output_dir")
        
#         # Validate required inputs
#         if not command:
#             raise ValueError("Missing required input: command")
#         if not output_dir:
#             raise ValueError("Missing required input: output_dir")
#         if not self.module_name:
#             raise ValueError("Module name must be provided during initialization")
                
#         # Create the output directory if it doesn't exist
#         os.makedirs(output_dir, exist_ok=True)
        
#         # Set up paths for the job script and output files
#         job_script_filename = f"{self.job_name}_job.sh"
#         job_script_path = os.path.join(output_dir, job_script_filename)
#         stdout_path = os.path.join(output_dir, f"{self.job_name}.out")
#         stderr_path = os.path.join(output_dir, f"{self.job_name}.err")
        
#         # Generate the job script
#         with open(job_script_path, 'w') as f:
#             f.write("#!/bin/bash\n")
            
#             # Add SLURM parameters
#             f.write(f"#SBATCH --job-name={self.job_name}\n")
#             f.write(f"#SBATCH --output={stdout_path}\n")
#             f.write(f"#SBATCH --error={stderr_path}\n")
#             f.write(f"#SBATCH --time={self.slurm_params['time']}\n")
#             f.write(f"#SBATCH --mem={self.slurm_params['mem']}\n")
#             f.write(f"#SBATCH --cpus-per-task={self.slurm_params['cpus_per_task']}\n")
            
#             # Add partition if specified
#             if self.slurm_params['partition']:
#                 f.write(f"#SBATCH --partition={self.slurm_params['partition']}\n")
            
#             f.write("\n")
            
#             # Load the module
#             f.write(f"module load {self.module_name}\n")
            
#             # Add the command
#             f.write(f"echo 'Running command: {command}'\n")
#             f.write(f"echo 'Started at: ' $(date)\n")
#             f.write(f"{command}\n")
#             f.write(f"EXIT_CODE=$?\n")
#             f.write(f"echo 'Finished at: ' $(date)\n")
#             f.write(f"echo 'Exit code: ' $EXIT_CODE\n")
#             f.write(f"exit $EXIT_CODE\n")
        
#         # Make the script executable
#         os.chmod(job_script_path, 0o755)
        
#         # Return the job script path
#         return {"job_script": job_script_path}

class JobExecutorPipe(FlowPipe):
    """
    Pipe for executing a single job and monitoring its completion.
    
    Input:
        - job_script: Path to the job script to execute #TODO change this to job path
        
    Output:
        Currently not used /-/ job_id: ID of the submitted job (if successful)
        Currently not used /-/ success: Boolean indicating if the job was submitted successfully
        - status: Final status of the job (if wait_for_completion is True)
    """
    
    def __init__(self, wait_for_completion=True, poll_interval=15):
        """Initialize the job executor pipe."""
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        super().__init__(inputs=["job_script"], outputs=["status"])#, "success", "status"])
    
    def submit_job(self, script_path):
        """Submit a job to SLURM and return the job ID."""
        try:
            result = subprocess.run(
                ["sbatch", script_path],
                capture_output=True, text=True, check=True
            )
            
            # Extract job ID from output (format: "Submitted batch job 12345")
            match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if match:
                job_id = match.group(1)
                print(f"Job submitted successfully with ID: {job_id}")
                return job_id
            else:
                print(f"Could not extract job ID from output: {result.stdout}")
                return None
        except subprocess.CalledProcessError as e:
            print(f"Error submitting job: {e.stderr}")
            return None
    
    def check_job_status(self, job_id):
        """Check the status of a SLURM job."""
        try:
            result = subprocess.run(
                ["sacct", "-j", job_id, "--format=JobID,State,ExitCode", "--noheader", "--parsable2"],
                capture_output=True, text=True, check=True
            )
            
            # Process the result (may have multiple lines for job steps)
            lines = [line for line in result.stdout.strip().split('\n') if line]
            for line in lines:
                parts = line.split('|')
                if len(parts) >= 2:
                    # Extract the main job ID (without .batch, etc.)
                    line_job_id = parts[0].split('.')[0]
                    if line_job_id == job_id:
                        state = parts[1]
                        return state
            
            return "UNKNOWN"
        except subprocess.CalledProcessError as e:
            print(f"Error checking job status: {e.stderr}")
            return "ERROR"
    
    def execute(self, data):
        """Execute a single job and monitor its completion if requested."""
        result = {}
        
        # Get required inputs
        job_script = data.get('job_script')
        
        # Validate required inputs
        if not job_script:
            raise ValueError("Missing required input: job_script")
        
        if not os.path.exists(job_script):
            raise ValueError(f"Job script not found: {job_script}")
        
        # Submit the job
        job_id = self.submit_job(job_script)
        
        # Initialize output
        #result['success'] = job_id is not None
        #result['job_script'] = job_script
        result["status"] = "NEW"
        if job_id:
            #result['job_id'] = job_id
            
            # Wait for completion if requested
            if self.wait_for_completion:
                status = None
                completed = False
                
                while not completed:
                    status = self.check_job_status(job_id)
                    print(f"Job {job_id} status: {status}")
                    
                    if status in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
                        completed = True
                    else:
                        time.sleep(self.poll_interval)
                
                result['status'] = status
                #result['completed'] = status == "COMPLETED"
        else:
            #status is Error, log the error
            result['status'] = "ERROR"
            print(f"Failed to submit job: {job_script}")
            
            #result['error'] = "Failed to submit job"
        return result

# For backward compatibility
class BatchJobExecutorPipe(FlowPipe):
    """
    DEPRECATED: Use JobExecutorPipe with FlowParallelPipe instead.
    This class is maintained for backward compatibility.
    
    Pipe for executing multiple jobs in parallel batches.
    
    Input:
        - job_scripts: List of paths to job scripts
        - max_concurrent: Maximum number of concurrent jobs (default: 5)
        - wait_for_completion: Whether to wait for all jobs to complete
        - poll_interval: Interval in seconds to poll for job status
        
    Output:
        - job_ids: List of submitted job IDs
        - failed_jobs: List of jobs that failed to submit
        - completed_jobs: Dictionary of completed jobs with their final status
    """
    
    def __init__(self):
        """Initialize the batch job executor pipe."""
        super().__init__()
    
    def submit_job(self, script_path):
        """Submit a job to SLURM and return the job ID."""
        try:
            result = subprocess.run(
                ["sbatch", script_path],
                capture_output=True, text=True, check=True
            )
            
            # Extract job ID from output (format: "Submitted batch job 12345")
            match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if match:
                job_id = match.group(1)
                print(f"Job submitted successfully with ID: {job_id}")
                return job_id
            else:
                print(f"Could not extract job ID from output: {result.stdout}")
                return None
        except subprocess.CalledProcessError as e:
            print(f"Error submitting job: {e.stderr}")
            return None
    
    def check_job_status(self, job_id):
        """Check the status of a SLURM job."""
        try:
            result = subprocess.run(
                ["sacct", "-j", job_id, "--format=JobID,State,ExitCode", "--noheader", "--parsable2"],
                capture_output=True, text=True, check=True
            )
            
            # Process the result (may have multiple lines for job steps)
            lines = [line for line in result.stdout.strip().split('\n') if line]
            for line in lines:
                parts = line.split('|')
                if len(parts) >= 2:
                    # Extract the main job ID (without .batch, etc.)
                    line_job_id = parts[0].split('.')[0]
                    if line_job_id == job_id:
                        state = parts[1]
                        return state
            
            return "UNKNOWN"
        except subprocess.CalledProcessError as e:
            print(f"Error checking job status: {e.stderr}")
            return "ERROR"
    
    def execute(self, data):
        """Execute multiple jobs in parallel batches."""
        result = {}
        
        # Pass through these values if present
        for key in ['stdout_path', 'stderr_path', 'output_dir']:
            if key in data:
                result[key] = data[key]
        
        # Get required inputs
        job_scripts = data.get('job_scripts', [])
        max_concurrent = data.get('max_concurrent', 5)
        wait_for_completion = data.get('wait_for_completion', False)
        poll_interval = data.get('poll_interval', 5)
        
        # Validate required inputs
        if not job_scripts:
            raise ValueError("Missing required input: job_scripts")
        
        # Initialize tracking variables
        job_ids = []
        failed_jobs = []
        active_jobs = {}  # job_id -> script_path
        completed_jobs = {}  # job_id -> status
        
        # Process jobs
        script_queue = list(job_scripts)  # Make a copy to use as a queue
        
        print(f"Processing {len(script_queue)} jobs with max {max_concurrent} concurrent jobs")
        
        while script_queue or active_jobs:
            # Submit jobs up to the max_concurrent limit
            while script_queue and len(active_jobs) < max_concurrent:
                script_path = script_queue.pop(0)
                job_id = self.submit_job(script_path)
                
                if job_id:
                    job_ids.append(job_id)
                    active_jobs[job_id] = script_path
                else:
                    failed_jobs.append(script_path)
                    print(f"Failed to submit job: {script_path}")
            
            # If we're not waiting for completion, exit the loop
            if not wait_for_completion and not script_queue:
                break
            
            # Check status of active jobs
            if active_jobs:
                completed_job_ids = []
                
                for job_id, script_path in list(active_jobs.items()):
                    status = self.check_job_status(job_id)
                    
                    if status in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
                        print(f"Job {job_id} ({os.path.basename(script_path)}) completed with status: {status}")
                        completed_jobs[job_id] = status
                        completed_job_ids.append(job_id)
                
                # Remove completed jobs from active jobs
                for job_id in completed_job_ids:
                    del active_jobs[job_id]
            
            # If there are still jobs to process, wait before checking again
            if active_jobs or script_queue:
                time.sleep(poll_interval)
        
        # Return results
        result['job_ids'] = job_ids
        result['failed_jobs'] = failed_jobs
        result['completed_jobs'] = completed_jobs
        
        return result
import random
from flowline import FlowPipe
import os
import subprocess
import time

class UnitAmountConverterPipe(FlowPipe):
    """
    FlowPipe to convert a potentially percentage-based amount into an absolute number.
    
    Inputs:
        - "items" (list): A list of items to calculate the number from.
        - "amount" (int or str): Specifies how many items to select.
          Can be:
            - An integer: Absolute number of items.
            - A percentage string (e.g., "10%"): Percentage of total items.
    
    Outputs:
        - "amount" (int): The calculated absolute amount.
    
    Raises:
        - ValueError if amount format is invalid.
    """
    
    def __init__(self):
        super().__init__(
            inputs=["items", "amount"],
            outputs=["amount"],
            action=self.convert_amount
        )
        
    def convert_amount(self, data):
        items = data["items"]
        amount = data["amount"]
        num_items = len(items)
        
        # Determine absolute amount
        if isinstance(amount, str) and amount.endswith("%"):
            try:
                percentage = float(amount.rstrip("%"))
                absolute_amount = int(round(num_items * percentage / 100))
            except Exception as e:
                raise ValueError(f"Invalid amount format: {amount}") from e
        else:
            absolute_amount = int(amount)
            
        # Ensure amount is not greater than available items
        absolute_amount = min(absolute_amount, num_items)
        
        return {"amount": absolute_amount}
        
    def __str__(self):
        """Returns a debug-friendly representation of the UnitAmountConverterPipe."""
        return "UnitAmountConverterPipe(Converting relative or absolute amount to absolute number)"

class CommandExecutorPipe(FlowPipe):
    """
    Pipe for executing a command line command directly and monitoring its completion.
    
    Input:
        - command: Command string to execute
        - output_dir: Directory to store output logs
        
    Output:
        - status: Completion status ("COMPLETED", "FAILED", "ERROR", etc.)
        - log_file: Path to the log file containing stdout/stderr
        - exit_code: The command's exit code
        (optionally, "error_message" is added in case of exceptions)
    """
    
    def __init__(self, log_prefix="command_execution"):
        """
        Initialize the command executor pipe.
        
        Args:
            log_prefix: Prefix for log filenames (default: "command_execution")
        """
        self.log_prefix = log_prefix
        super().__init__(
            inputs=["command", "output_dir"],
            outputs=["status", "log_file", "exit_code"],
            action=self.execute_command
        )
        
    def execute_command(self, data):
        """Execute the command and monitor its completion."""
        # Extract required inputs
        command = data.get("command")
        output_dir = data.get("output_dir")
        
        # Validate required inputs
        if not command:
            raise ValueError("Missing required input: command")
        if not output_dir:
            raise ValueError("Missing required input: output_dir")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up paths for log files using a timestamp for uniqueness
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(output_dir, f"{self.log_prefix}_{timestamp}.log")
        
        print(f"Executing command: {command}")
        print(f"Output will be logged to: {log_file}")
        
        # Initialize result dictionary
        result = {
            "status": "UNKNOWN",
            "log_file": log_file,
            "exit_code": None
        }
        
        try:
            # Open log file for writing
            with open(log_file, "w") as log:
                # Write header information to log
                log.write(f"Command: {command}\n")
                log.write(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                log.write("=" * 80 + "\n\n")
                log.flush()
                
                # Execute the command with output redirected to stdout and stderr
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1  # Line buffered
                )
                
                # Stream output to the log file in real time
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                
                # Wait for the process to complete
                exit_code = process.wait()
                
                # Record completion information
                log.write("\n" + "=" * 80 + "\n")
                log.write(f"Command finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                log.write(f"Exit code: {exit_code}\n")
                
                # Update result based on exit code
                result["exit_code"] = exit_code
                result["status"] = "COMPLETED" if exit_code == 0 else "FAILED"
                    
        except Exception as e:
            # Handle any exceptions during execution
            with open(log_file, "a") as log:
                log.write(f"\nException occurred: {str(e)}\n")
            result["status"] = "ERROR"
            result["error_message"] = str(e)
            print(f"Error executing command: {e}")
            
        print(f"Command execution completed with status: {result['status']}")
        return result
        
    def __str__(self):
        """Returns a debug-friendly representation of the CommandExecutorPipe."""
        return "CommandExecutorPipe(Executing command and monitoring completion)"

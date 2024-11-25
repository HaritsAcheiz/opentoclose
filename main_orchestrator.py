import logging
import time
from datetime import datetime
import subprocess
import sys
import os

# Configure logging
logging.basicConfig(
    filename='script_orchestrator.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ScriptOrchestrator:
    def __init__(self):
        self.scripts = [
            'main.py',
            'tc_payroll/main.py',
            'daily_contract_count/main.py',
            'tc_daily_update/main.py'
        ]
        self.script_dir = os.path.dirname(os.path.abspath(__file__))

    def run_script(self, script_name):
        """Run a single Python script and return success status"""
        try:
            script_path = os.path.join(self.script_dir, script_name)
            logging.info(f"Starting {script_name}")
            
            # Run script using Python interpreter
            process = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            logging.info(f"Successfully completed {script_name}")
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running {script_name}: {str(e)}")
            logging.error(f"Script output: {e.output}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error running {script_name}: {str(e)}")
            return False

    def run_sequence(self):
        """Run all scripts in sequence"""
        logging.info("Starting script sequence")
        
        for script in self.scripts:
            success = self.run_script(script)
            if not success:
                logging.error(f"Sequence failed at {script}")
                return False
            
        logging.info("Successfully completed full sequence")
        return True


if __name__ == "__main__":
    orchestrator = ScriptOrchestrator()
    orchestrator.run_sequence()
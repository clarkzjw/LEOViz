import json
import logging
import subprocess
from typing import Optional, Dict, Any

import config

logger = logging.getLogger(__name__)

GRPC_TIMEOUT = 10

class GrpcCommand:
    """Handles GRPC command execution and response parsing."""
    
    def __init__(self, command_type: str, data: str):
        self.command_type = command_type
        self.data = data
        self.cmd = [
            "grpcurl",
            "-plaintext",
            "-d",
            data,
            config.STARLINK_GRPC_ADDR_PORT,
            "SpaceX.API.Device.Device/Handle",
        ]

    def execute(self) -> Optional[Dict[str, Any]]:
        """Execute the GRPC command and return parsed response."""
        try:
            output = subprocess.check_output(self.cmd, timeout=GRPC_TIMEOUT)
            return json.loads(output.decode("utf-8"))
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout executing {self.command_type} command")
            return None
        except Exception as e:
            logger.error(f"Error executing {self.command_type} command: {str(e)}")
            return None

    def save_to_file(self, filename: str) -> None:
        """Save command output to a file."""
        try:
            with open(filename, "w") as outfile:
                subprocess.run(self.cmd, stdout=outfile, timeout=GRPC_TIMEOUT)
            logger.info(f"Saved {self.command_type} to {filename}")
        except subprocess.TimeoutExpired:
            pass 
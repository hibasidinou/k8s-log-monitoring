#!/usr/bin/env python3
"""
Main controller script for K8s Real-Time Data Collector and Simulator
Replaces all shell scripts with pure Python implementation
"""

import subprocess
import sys
import os
import time
import signal
import argparse
from pathlib import Path

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[0;32m'
    BLUE = '\033[0;34m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    NC = '\033[0m'  # No Color
    BOLD = '\033[1m'

class SystemController:
    def __init__(self):
        self.processes = []
        self.script_dir = Path(__file__).parent.resolve()
        self.project_root = self.script_dir.parent
        
    def print_colored(self, message, color=Colors.NC):
        """Print colored message to terminal"""
        print(f"{color}{message}{Colors.NC}")
        
    def check_python_version(self):
        """Verify Python version is 3.7+"""
        if sys.version_info < (3, 7):
            self.print_colored("ERROR: Python 3.7 or higher is required!", Colors.RED)
            self.print_colored(f"Current version: {sys.version}", Colors.RED)
            return False
        return True
    
    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        self.print_colored("\n=== Checking Directories ===", Colors.BLUE)
        
        required_dirs = [
            'data/real_time/system_logs',
            'data/real_time/security_logs',
            'data/real_time/raw_buffer'
        ]
        
        for dir_path in required_dirs:
            full_path = self.project_root / dir_path
            if not full_path.exists():
                self.print_colored(f"Creating: {dir_path}", Colors.YELLOW)
                full_path.mkdir(parents=True, exist_ok=True)
            else:
                self.print_colored(f"Found: {dir_path}", Colors.GREEN)
        
        self.print_colored("Directories ready!", Colors.GREEN)
        return True
    
    def quick_start(self):
        """Full quick start: setup and run everything"""
        self.print_colored(f"\n{Colors.BOLD}=== K8s Real-Time System Quick Start ==={Colors.NC}", Colors.GREEN)
        
        if not self.check_python_version():
            return False
        
        if not self.ensure_directories():
            return False
        
        # Start collector
        collector = self.start_collector(wait=True)
        if not collector:
            return False
        
        # Start simulator
        simulator = self.start_simulator(test_mode=False)
        if not simulator:
            self.stop_all_processes()
            return False
        
        # Monitor processes
        self.monitor_processes()
        
        # Cleanup
        self.stop_all_processes()
        return True
    
    def start_collector(self, wait=True):
        """Start the data collector API"""
        self.print_colored("\n=== Starting Data Collector ===", Colors.BLUE)
        collector_script = self.script_dir / "real_time_collector.py"
        
        if not collector_script.exists():
            self.print_colored("ERROR: real_time_collector.py not found!", Colors.RED)
            return None
            
        try:
            process = subprocess.Popen(
                [sys.executable, str(collector_script)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            self.processes.append(('collector', process))
            
            if wait:
                self.print_colored("Waiting for API to start...", Colors.YELLOW)
                time.sleep(3)  # Give Flask time to start
                
                if process.poll() is None:
                    self.print_colored("Collector API started successfully!", Colors.GREEN)
                    self.print_colored("API running at: http://localhost:5000", Colors.GREEN)
                else:
                    self.print_colored("ERROR: Collector failed to start!", Colors.RED)
                    return None
                    
            return process
        except Exception as e:
            self.print_colored(f"ERROR: Failed to start collector: {e}", Colors.RED)
            return None
    
    def start_simulator(self, test_mode=False):
        """Start the K8s log simulator"""
        self.print_colored("\n=== Starting Log Simulator ===", Colors.BLUE)
        simulator_script = self.script_dir / "k8s_simulator.py"
        
        if not simulator_script.exists():
            self.print_colored("ERROR: k8s_simulator.py not found!", Colors.RED)
            return None
            
        try:
            args = [sys.executable, str(simulator_script)]
            if test_mode:
                args.append("--test")
            else:
                args.append("--run")
                
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            self.processes.append(('simulator', process))
            
            time.sleep(1)
            if process.poll() is None:
                self.print_colored("Simulator started successfully!", Colors.GREEN)
            else:
                self.print_colored("ERROR: Simulator failed to start!", Colors.RED)
                return None
                
            return process
        except Exception as e:
            self.print_colored(f"ERROR: Failed to start simulator: {e}", Colors.RED)
            return None
    
    def monitor_processes(self):
        """Monitor running processes and display output"""
        self.print_colored("\n=== System Running ===", Colors.GREEN)
        self.print_colored("Press Ctrl+C to stop all processes", Colors.YELLOW)
        self.print_colored("-" * 50, Colors.BLUE)
        
        try:
            while True:
                for name, process in self.processes:
                    if process.poll() is not None:
                        self.print_colored(f"\nWARNING: {name} process terminated!", Colors.RED)
                        return False
                        
                    # Read and display output
                    if process.stdout:
                        line = process.stdout.readline()
                        if line:
                            print(f"[{name}] {line.strip()}")
                            
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            self.print_colored("\n\nReceived shutdown signal...", Colors.YELLOW)
            return True
    
    def stop_all_processes(self):
        """Stop all running processes gracefully"""
        self.print_colored("\n=== Stopping All Processes ===", Colors.BLUE)
        
        for name, process in self.processes:
            if process.poll() is None:
                self.print_colored(f"Stopping {name}...", Colors.YELLOW)
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    self.print_colored(f"{name} stopped successfully", Colors.GREEN)
                except subprocess.TimeoutExpired:
                    self.print_colored(f"Force killing {name}...", Colors.RED)
                    process.kill()
                except Exception as e:
                    self.print_colored(f"ERROR stopping {name}: {e}", Colors.RED)
        
        self.processes.clear()
        self.print_colored("\nAll processes stopped", Colors.GREEN)
    
    def run_collector_only(self):
        """Run only the data collector"""
        self.print_colored(f"\n{Colors.BOLD}=== Starting Collector Only ==={Colors.NC}", Colors.BLUE)
        
        if not self.check_python_version():
            return False
            
        collector = self.start_collector(wait=False)
        if not collector:
            return False
        
        try:
            # Stream output
            for line in collector.stdout:
                print(line.strip())
        except KeyboardInterrupt:
            self.print_colored("\n\nStopping collector...", Colors.YELLOW)
            self.stop_all_processes()
        
        return True
    
    def run_simulator_only(self, test_mode=False):
        """Run only the simulator"""
        mode = "Test Mode" if test_mode else "Live Mode"
        self.print_colored(f"\n{Colors.BOLD}=== Starting Simulator ({mode}) ==={Colors.NC}", Colors.BLUE)
        
        if not self.check_python_version():
            return False
            
        simulator = self.start_simulator(test_mode=test_mode)
        if not simulator:
            return False
        
        try:
            # Stream output
            for line in simulator.stdout:
                print(line.strip())
        except KeyboardInterrupt:
            self.print_colored("\n\nStopping simulator...", Colors.YELLOW)
            self.stop_all_processes()
        
        return True

def main():
    parser = argparse.ArgumentParser(
        description="K8s Real-Time Data Collector and Simulator Controller",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_system.py                    # Full system quick start
  python run_system.py --quick            # Quick start (default)
  python run_system.py --collector        # Run collector only
  python run_system.py --simulator        # Run simulator only
  python run_system.py --test             # Run simulator in test mode
        """
    )
    
    parser.add_argument('--quick', '-q', action='store_true',
                       help='Run full quick start (default)')
    parser.add_argument('--collector', '-c', action='store_true',
                       help='Run collector only')
    parser.add_argument('--simulator', '-m', action='store_true',
                       help='Run simulator only')
    parser.add_argument('--test', '-t', action='store_true',
                       help='Run simulator in test mode (no API calls)')
    
    args = parser.parse_args()
    
    controller = SystemController()
    
    try:
        if args.collector:
            success = controller.run_collector_only()
        elif args.simulator or args.test:
            success = controller.run_simulator_only(test_mode=args.test)
        else:
            # Default: quick start
            success = controller.quick_start()
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        controller.print_colored(f"\nFATAL ERROR: {e}", Colors.RED)
        controller.stop_all_processes()
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Comprehensive testing script for K8s Real-Time System
Tests all components and validates functionality
"""

import sys
import time
import requests
from pathlib import Path
import subprocess
import json

class Colors:
    GREEN = '\033[0;32m'
    BLUE = '\033[0;34m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    NC = '\033[0m'
    BOLD = '\033[1m'

class SystemTester:
    def __init__(self):
        self.script_dir = Path(__file__).parent.resolve()
        self.tests_passed = 0
        self.tests_failed = 0
        
    def print_colored(self, message, color=Colors.NC):
        print(f"{color}{message}{Colors.NC}")
    
    def print_test(self, test_name, passed, details=""):
        if passed:
            self.tests_passed += 1
            self.print_colored(f"✓ {test_name}", Colors.GREEN)
        else:
            self.tests_failed += 1
            self.print_colored(f"✗ {test_name}", Colors.RED)
        if details:
            self.print_colored(f"  {details}", Colors.YELLOW)
    
    def test_python_version(self):
        """Test 1: Python version"""
        self.print_colored("\n[1/6] Testing Python version...", Colors.BLUE)
        passed = sys.version_info >= (3, 7)
        self.print_test(
            "Python 3.7+",
            passed,
            f"Found: Python {sys.version.split()[0]}"
        )
        return passed
    
    def test_directory_structure(self):
        """Test 2: Directory structure"""
        self.print_colored("\n[2/6] Testing directory structure...", Colors.BLUE)
        
        required_dirs = [
            'data',
            'data/real_time',
            'data/real_time/system_logs',
            'data/real_time/security_logs',
            'data/real_time/raw_buffer'
        ]
        
        all_exist = True
        project_root = self.script_dir.parent
        
        for dir_path in required_dirs:
            full_path = project_root / dir_path
            exists = full_path.exists() and full_path.is_dir()
            self.print_test(f"Directory: {dir_path}", exists)
            if not exists:
                all_exist = False
        
        return all_exist
    
    def test_script_files(self):
        """Test 3: Script files exist"""
        self.print_colored("\n[3/6] Testing script files...", Colors.BLUE)
        
        required_files = [
            'real_time_collector.py',
            'k8s_simulator.py'
        ]
        
        all_exist = True
        for file_name in required_files:
            file_path = self.script_dir / file_name
            exists = file_path.exists() and file_path.is_file()
            self.print_test(f"File: {file_name}", exists)
            if not exists:
                all_exist = False
        
        return all_exist
    
    def test_simulator_standalone(self):
        """Test 4: Simulator in test mode"""
        self.print_colored("\n[4/6] Testing simulator (standalone)...", Colors.BLUE)
        
        simulator_script = self.script_dir / "k8s_simulator.py"
        
        try:
            result = subprocess.run(
                [sys.executable, str(simulator_script), "--test"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            passed = result.returncode == 0
            self.print_test(
                "Simulator test mode",
                passed,
                "Check output for any errors" if not passed else "Passed validation"
            )
            return passed
        except subprocess.TimeoutExpired:
            self.print_test("Simulator test mode", False, "Timeout")
            return False
        except Exception as e:
            self.print_test("Simulator test mode", False, str(e))
            return False
    
    def test_collector_api_start(self):
        """Test 5: Collector API startup"""
        self.print_colored("\n[5/6] Testing collector API startup...", Colors.BLUE)
        
        collector_script = self.script_dir / "real_time_collector.py"
        
        try:
            # Start collector
            process = subprocess.Popen(
                [sys.executable, str(collector_script)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for startup
            time.sleep(3)
            
            # Check if still running
            if process.poll() is not None:
                self.print_test("Collector startup", False, "Process terminated early")
                return False, None
            
            self.print_test("Collector startup", True, "API started successfully")
            return True, process
            
        except Exception as e:
            self.print_test("Collector startup", False, str(e))
            return False, None
    
    def test_collector_api_endpoints(self, process):
        """Test 6: Collector API endpoints"""
        self.print_colored("\n[6/6] Testing collector API endpoints...", Colors.BLUE)
        
        base_url = "http://localhost:5000"
        
        # Test health endpoint
        try:
            response = requests.get(f"{base_url}/api/health", timeout=5)
            health_passed = response.status_code == 200
            self.print_test("GET /api/health", health_passed)
        except Exception as e:
            self.print_test("GET /api/health", False, str(e))
            health_passed = False
        
        # Test stats endpoint
        try:
            response = requests.get(f"{base_url}/api/stats", timeout=5)
            stats_passed = response.status_code == 200
            self.print_test("GET /api/stats", stats_passed)
        except Exception as e:
            self.print_test("GET /api/stats", False, str(e))
            stats_passed = False
        
        # Test log ingestion
        try:
            test_log = {
                "timestamp": time.time(),
                "log_type": "system",
                "message": "Test log entry",
                "node": "test-node",
                "pod": "test-pod"
            }
            response = requests.post(
                f"{base_url}/api/log",
                json=test_log,
                timeout=5
            )
            ingest_passed = response.status_code == 200
            self.print_test("POST /api/log", ingest_passed)
        except Exception as e:
            self.print_test("POST /api/log", False, str(e))
            ingest_passed = False
        
        return health_passed and stats_passed and ingest_passed
    
    def run_all_tests(self):
        """Run complete test suite"""
        self.print_colored(f"\n{Colors.BOLD}=== K8s Real-Time System Test Suite ==={Colors.NC}", Colors.BLUE)
        self.print_colored("Running 6 comprehensive system tests...\n", Colors.YELLOW)
        
        collector_process = None
        
        try:
            # Tests 1-4: Basic checks (no API needed)
            self.test_python_version()
            self.test_directory_structure()
            self.test_script_files()
            self.test_simulator_standalone()
            
            # Tests 5-6: API tests
            api_started, collector_process = self.test_collector_api_start()
            
            if api_started:
                self.test_collector_api_endpoints(collector_process)
            else:
                self.print_colored("\nSkipping API tests (collector not running)", Colors.YELLOW)
                self.tests_failed += 2
            
        finally:
            # Cleanup
            if collector_process and collector_process.poll() is None:
                self.print_colored("\nStopping collector...", Colors.YELLOW)
                collector_process.terminate()
                try:
                    collector_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    collector_process.kill()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        total = self.tests_passed + self.tests_failed
        
        self.print_colored(f"\n{'='*50}", Colors.BLUE)
        self.print_colored(f"{Colors.BOLD}Test Summary{Colors.NC}", Colors.BLUE)
        self.print_colored(f"{'='*50}", Colors.BLUE)
        
        self.print_colored(f"Total Tests: {total}", Colors.BLUE)
        self.print_colored(f"Passed: {self.tests_passed}", Colors.GREEN)
        self.print_colored(f"Failed: {self.tests_failed}", Colors.RED)
        
        if self.tests_failed == 0:
            self.print_colored(f"\n{Colors.BOLD}✓ ALL TESTS PASSED!{Colors.NC}", Colors.GREEN)
            self.print_colored("System is ready to use.", Colors.GREEN)
        else:
            self.print_colored(f"\n{Colors.BOLD}✗ SOME TESTS FAILED{Colors.NC}", Colors.RED)
            self.print_colored("Please fix the issues above before running the system.", Colors.YELLOW)

def main():
    tester = SystemTester()
    tester.run_all_tests()
    
    sys.exit(0 if tester.tests_failed == 0 else 1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import unittest
import sys
import os

# Add the examples directory to the path so we can import the linters
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'examples'))

from WDL.Lint import test_linter, create_test_wdl
from example_linters.style_linters import (
    TaskNamingLinter, WorkflowNamingLinter, DocumentationLinter,
    IndentationLinter, VariableNamingLinter
)
from example_linters.security_linters import (
    DangerousCommandLinter, CredentialScannerLinter, NetworkAccessLinter,
    FilePermissionLinter, InputValidationLinter as SecurityInputValidationLinter
)
from example_linters.performance_linters import (
    ResourceAllocationLinter, InefficientCommandLinter, LargeFileHandlingLinter,
    ParallelizationLinter, IOOptimizationLinter, MemoryUsageLinter
)
from example_linters.best_practices_linters import (
    WorkflowStructureLinter, ErrorHandlingLinter, OutputOrganizationLinter,
    InputValidationLinter as BestPracticesInputValidationLinter,
    VersioningLinter, ModularityLinter, ConsistencyLinter
)


class TestStyleLinters(unittest.TestCase):
    def test_task_naming_linter(self):
        """Test TaskNamingLinter with various naming patterns"""
        
        # Test good naming (should pass)
        test_linter(
            TaskNamingLinter,
            """
            task good_task_name {
              command { echo "hello" }
            }
            """,
            expected_lint=[]
        )
        
        # Test bad naming (camelCase)
        test_linter(
            TaskNamingLinter,
            """
            task BadTaskName {
              command { echo "hello" }
            }
            """,
            expected_lint=["should use snake_case"]
        )
        
        # Test too short name
        test_linter(
            TaskNamingLinter,
            """
            task ab {
              command { echo "hello" }
            }
            """,
            expected_lint=["too short"]
        )
        
        # Test generic name
        test_linter(
            TaskNamingLinter,
            """
            task run {
              command { echo "hello" }
            }
            """,
            expected_lint=["too generic"]
        )
    
    def test_documentation_linter(self):
        """Test DocumentationLinter"""
        
        # Test missing documentation
        test_linter(
            DocumentationLinter,
            """
            task undocumented_task {
              command { echo "hello" }
            }
            """,
            expected_lint=["should have a description"]
        )
        
        # Test with good documentation
        test_linter(
            DocumentationLinter,
            """
            task documented_task {
              meta {
                description: "This task processes input data and generates output"
              }
              command { echo "hello" }
            }
            """,
            expected_lint=[]
        )


class TestSecurityLinters(unittest.TestCase):
    def test_dangerous_command_linter(self):
        """Test DangerousCommandLinter"""
        
        # Test safe command (should pass)
        test_linter(
            DangerousCommandLinter,
            """
            task safe_task {
              command { echo "hello world" }
            }
            """,
            expected_lint=[]
        )
        
        # Test dangerous rm command
        test_linter(
            DangerousCommandLinter,
            """
            task dangerous_task {
              command { rm -rf /data }
            }
            """,
            expected_lint=["Potentially dangerous command"]
        )
        
        # Test sudo usage
        test_linter(
            DangerousCommandLinter,
            """
            task sudo_task {
              command { sudo apt-get install package }
            }
            """,
            expected_lint=["Potentially dangerous command"]
        )
    
    def test_credential_scanner_linter(self):
        """Test CredentialScannerLinter"""
        
        # Test safe command (should pass)
        test_linter(
            CredentialScannerLinter,
            """
            task safe_task {
              input {
                String password
              }
              command { mysql -u user -p~{password} }
            }
            """,
            expected_lint=[]
        )
        
        # Test hardcoded password
        test_linter(
            CredentialScannerLinter,
            """
            task unsafe_task {
              command { mysql -u user -ppassword123 }
            }
            """,
            expected_lint=["Potential hardcoded credential"]
        )
    
    def test_network_access_linter(self):
        """Test NetworkAccessLinter"""
        
        # Test safe command (should pass)
        test_linter(
            NetworkAccessLinter,
            """
            task safe_task {
              command { echo "hello" }
            }
            """,
            expected_lint=[]
        )
        
        # Test network command
        test_linter(
            NetworkAccessLinter,
            """
            task network_task {
              command { wget https://example.com/file }
            }
            """,
            expected_lint=["network command"]
        )
        
        # Test insecure protocol
        test_linter(
            NetworkAccessLinter,
            """
            task insecure_task {
              command { wget http://example.com/file }
            }
            """,
            expected_count=2  # Both network command and insecure protocol
        )


class TestPerformanceLinters(unittest.TestCase):
    def test_resource_allocation_linter(self):
        """Test ResourceAllocationLinter"""
        
        # Test missing resources
        test_linter(
            ResourceAllocationLinter,
            """
            task no_resources {
              command { echo "hello" }
            }
            """,
            expected_lint=["should specify memory", "should specify CPU"]
        )
        
        # Test with good resources
        test_linter(
            ResourceAllocationLinter,
            """
            task good_resources {
              command { echo "hello" }
              runtime {
                memory: "4 GB"
                cpu: 2
                disk: "10 GB"
              }
            }
            """,
            expected_lint=[]
        )
    
    def test_inefficient_command_linter(self):
        """Test InefficientCommandLinter"""
        
        # Test efficient command (should pass)
        test_linter(
            InefficientCommandLinter,
            """
            task efficient_task {
              command { head -10 file.txt }
            }
            """,
            expected_lint=[]
        )
        
        # Test inefficient command
        test_linter(
            InefficientCommandLinter,
            """
            task inefficient_task {
              command { cat file.txt | head -10 }
            }
            """,
            expected_lint=["Use 'head file' instead"]
        )


class TestBestPracticesLinters(unittest.TestCase):
    def test_workflow_structure_linter(self):
        """Test WorkflowStructureLinter"""
        
        # Test simple workflow (should pass)
        test_linter(
            WorkflowStructureLinter,
            """
            workflow simple_workflow {
              meta {
                description: "A simple workflow"
                version: "1.0"
              }
              input {
                String sample_name
              }
              output {
                String result = sample_name
              }
            }
            """,
            expected_lint=[]
        )
        
        # Test workflow without documentation
        test_linter(
            WorkflowStructureLinter,
            """
            workflow undocumented_workflow {
              input {
                String sample_name
              }
              output {
                String result = sample_name
              }
            }
            """,
            expected_lint=["should have a description", "should include version"]
        )
    
    def test_error_handling_linter(self):
        """Test ErrorHandlingLinter"""
        
        # Test good error handling
        test_linter(
            ErrorHandlingLinter,
            """
            task good_error_handling {
              command {
                set -euo pipefail
                wget https://example.com/file
              }
            }
            """,
            expected_lint=[]
        )
        
        # Test missing error handling
        test_linter(
            ErrorHandlingLinter,
            """
            task poor_error_handling {
              command {
                wget https://example.com/file
                process_file file
              }
            }
            """,
            expected_lint=["lacks error handling"]
        )
    
    def test_output_organization_linter(self):
        """Test OutputOrganizationLinter"""
        
        # Test good outputs
        test_linter(
            OutputOrganizationLinter,
            """
            task good_outputs {
              command { echo "hello" > analysis_report.txt }
              output {
                File analysis_report = "analysis_report.txt"
                String execution_log = stdout()
              }
            }
            """,
            expected_lint=[]
        )
        
        # Test generic output names
        test_linter(
            OutputOrganizationLinter,
            """
            task poor_outputs {
              command { echo "hello" > result.txt }
              output {
                File out = "result.txt"
                String s = stdout()
              }
            }
            """,
            expected_lint=["too generic", "too short"]
        )


class TestLinterIntegration(unittest.TestCase):
    def test_multiple_linters_together(self):
        """Test running multiple linters on the same WDL code"""
        
        # Create WDL that triggers multiple linters
        wdl_code = """
        task BadTaskName {
          input {
            String s
          }
          command {
            rm -rf /tmp/data
            cat file.txt | head -10
          }
          output {
            File out = "result.txt"
          }
        }
        """
        
        # Test style linter
        style_results = test_linter(TaskNamingLinter, wdl_code, expected_count=1)
        self.assertTrue(any("snake_case" in result[2] for result in style_results))
        
        # Test security linter
        security_results = test_linter(DangerousCommandLinter, wdl_code, expected_count=1)
        self.assertTrue(any("dangerous" in result[2].lower() for result in security_results))
        
        # Test performance linter
        perf_results = test_linter(InefficientCommandLinter, wdl_code, expected_count=1)
        self.assertTrue(any("head file" in result[2] for result in perf_results))
        
        # Test best practices linter
        bp_results = test_linter(OutputOrganizationLinter, wdl_code, expected_count=1)
        self.assertTrue(any("generic" in result[2] for result in bp_results))
    
    def test_linter_discovery_and_loading(self):
        """Test that linters can be discovered and loaded from the package"""
        
        # Test importing the package
        import example_linters
        
        # Check that all expected linters are available
        self.assertTrue(hasattr(example_linters, 'TaskNamingLinter'))
        self.assertTrue(hasattr(example_linters, 'DangerousCommandLinter'))
        self.assertTrue(hasattr(example_linters, 'ResourceAllocationLinter'))
        self.assertTrue(hasattr(example_linters, 'WorkflowStructureLinter'))
        
        # Check that collections are available
        self.assertTrue(hasattr(example_linters, 'STYLE_LINTERS'))
        self.assertTrue(hasattr(example_linters, 'SECURITY_LINTERS'))
        self.assertTrue(hasattr(example_linters, 'PERFORMANCE_LINTERS'))
        self.assertTrue(hasattr(example_linters, 'BEST_PRACTICES_LINTERS'))
        self.assertTrue(hasattr(example_linters, 'ALL_LINTERS'))
        
        # Verify collections contain the expected linters
        self.assertIn(TaskNamingLinter, example_linters.STYLE_LINTERS)
        self.assertIn(DangerousCommandLinter, example_linters.SECURITY_LINTERS)
        self.assertIn(ResourceAllocationLinter, example_linters.PERFORMANCE_LINTERS)
        self.assertIn(WorkflowStructureLinter, example_linters.BEST_PRACTICES_LINTERS)
    
    def test_real_world_wdl_example(self):
        """Test linters with a more realistic WDL example"""
        
        wdl_code = """
        version 1.0
        
        workflow data_processing {
          meta {
            description: "Processes genomic data through quality control and analysis"
            version: "2.1.0"
            author: "Genomics Team"
          }
          
          input {
            File input_fastq
            String sample_name
            Int? quality_threshold = 30
          }
          
          call quality_control {
            input:
              fastq_file = input_fastq,
              sample_id = sample_name,
              min_quality = select_first([quality_threshold, 30])
          }
          
          call sequence_analysis {
            input:
              qc_fastq = quality_control.filtered_fastq,
              sample_id = sample_name
          }
          
          output {
            File quality_report = quality_control.qc_report
            File analysis_results = sequence_analysis.analysis_output
            String processing_log = sequence_analysis.execution_log
          }
        }
        
        task quality_control {
          meta {
            description: "Performs quality control on FASTQ files"
            version: "1.0.0"
          }
          
          input {
            File fastq_file
            String sample_id
            Int min_quality
          }
          
          command {
            set -euo pipefail
            
            # Validate input file
            if [ ! -f "~{fastq_file}" ]; then
              echo "Error: Input FASTQ file not found" >&2
              exit 1
            fi
            
            # Run quality control
            fastqc "~{fastq_file}" -o qc_output/
            
            # Filter by quality
            seqtk trimfq -q ~{min_quality} "~{fastq_file}" > "~{sample_id}_filtered.fastq"
            
            # Generate report
            echo "Quality control completed for ~{sample_id}" > qc_report.txt
          }
          
          output {
            File filtered_fastq = "~{sample_id}_filtered.fastq"
            File qc_report = "qc_report.txt"
          }
          
          runtime {
            memory: "4 GB"
            cpu: 2
            disk: "20 GB"
          }
        }
        
        task sequence_analysis {
          meta {
            description: "Performs sequence analysis on quality-controlled data"
            version: "1.0.0"
          }
          
          input {
            File qc_fastq
            String sample_id
          }
          
          command {
            set -euo pipefail
            
            # Run analysis
            python3 analyze_sequences.py --input "~{qc_fastq}" --output "~{sample_id}_analysis.json"
            
            echo "Analysis completed successfully" > analysis.log
          }
          
          output {
            File analysis_output = "~{sample_id}_analysis.json"
            String execution_log = stdout()
          }
          
          runtime {
            memory: "8 GB"
            cpu: 4
            disk: "30 GB"
          }
        }
        """
        
        # This well-written WDL should pass most linters
        # Test a few key linters
        
        # Style linters should mostly pass
        style_results = test_linter(TaskNamingLinter, wdl_code, expected_lint=[])
        doc_results = test_linter(DocumentationLinter, wdl_code, expected_lint=[])
        
        # Security linters should pass (good practices used)
        security_results = test_linter(DangerousCommandLinter, wdl_code, expected_lint=[])
        
        # Performance linters should pass (resources specified)
        perf_results = test_linter(ResourceAllocationLinter, wdl_code, expected_lint=[])
        
        # Best practices should mostly pass
        error_results = test_linter(ErrorHandlingLinter, wdl_code, expected_lint=[])
        output_results = test_linter(OutputOrganizationLinter, wdl_code, expected_lint=[])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""
Demonstration of the example linters package

This script shows how to use the example linters with various WDL code samples
to demonstrate different types of issues that can be detected.
"""

import sys
import os

# Add the examples directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from WDL.Lint import test_linter
from example_linters import (
    TaskNamingLinter, DangerousCommandLinter, ResourceAllocationLinter,
    WorkflowStructureLinter, ErrorHandlingLinter
)


def demo_style_linters():
    """Demonstrate style linters"""
    print("🎨 Style Linters Demo")
    print("=" * 50)
    
    # Bad task naming
    print("\n1. Task Naming Issues:")
    bad_naming_wdl = """
    task ProcessData {
      command { echo "processing" }
    }
    """
    
    results = test_linter(TaskNamingLinter, bad_naming_wdl, expected_count=1)
    print(f"   Issue found: {results[0][2]}")
    
    # Good task naming
    print("\n   Fixed version:")
    good_naming_wdl = """
    task process_data {
      meta {
        description: "Processes input data and generates summary statistics"
      }
      command { echo "processing" }
    }
    """
    
    results = test_linter(TaskNamingLinter, good_naming_wdl, expected_lint=[])
    print("   ✅ No issues found!")


def demo_security_linters():
    """Demonstrate security linters"""
    print("\n🔒 Security Linters Demo")
    print("=" * 50)
    
    # Dangerous command
    print("\n1. Dangerous Command Detection:")
    dangerous_wdl = """
    task cleanup_data {
      command { rm -rf /tmp/analysis_data }
    }
    """
    
    results = test_linter(DangerousCommandLinter, dangerous_wdl, expected_count=1)
    print(f"   Issue found: {results[0][2]}")
    
    # Safe alternative
    print("\n   Safer alternative:")
    safe_wdl = """
    task cleanup_data {
      input {
        String data_dir
      }
      command {
        # Validate the directory path first
        if [[ "~{data_dir}" == /tmp/analysis_data/* ]]; then
          rm -rf "~{data_dir}"
        else
          echo "Error: Invalid data directory path" >&2
          exit 1
        fi
      }
    }
    """
    
    results = test_linter(DangerousCommandLinter, safe_wdl, expected_lint=[])
    print("   ✅ No issues found!")


def demo_performance_linters():
    """Demonstrate performance linters"""
    print("\n⚡ Performance Linters Demo")
    print("=" * 50)
    
    # Missing resource specifications
    print("\n1. Missing Resource Specifications:")
    no_resources_wdl = """
    task analyze_genome {
      command { 
        bwa mem reference.fa reads.fastq > aligned.sam
        samtools sort aligned.sam > aligned.bam
      }
    }
    """
    
    results = test_linter(ResourceAllocationLinter, no_resources_wdl, expected_count=3)
    for result in results:
        print(f"   Issue found: {result[2]}")
    
    # With proper resources
    print("\n   With proper resource allocation:")
    with_resources_wdl = """
    task analyze_genome {
      command { 
        bwa mem reference.fa reads.fastq > aligned.sam
        samtools sort aligned.sam > aligned.bam
      }
      runtime {
        memory: "8 GB"
        cpu: 4
        disk: "50 GB"
      }
    }
    """
    
    results = test_linter(ResourceAllocationLinter, with_resources_wdl, expected_lint=[])
    print("   ✅ No issues found!")


def demo_best_practices_linters():
    """Demonstrate best practices linters"""
    print("\n📋 Best Practices Linters Demo")
    print("=" * 50)
    
    # Poor error handling
    print("\n1. Error Handling Issues:")
    poor_error_handling_wdl = """
    task download_and_process {
      command {
        wget https://example.com/data.txt
        process_data data.txt
        generate_report processed_data.txt
      }
    }
    """
    
    results = test_linter(ErrorHandlingLinter, poor_error_handling_wdl, expected_count=1)
    print(f"   Issue found: {results[0][2]}")
    
    # Good error handling
    print("\n   With proper error handling:")
    good_error_handling_wdl = """
    task download_and_process {
      command <<<
        set -euo pipefail
        
        wget https://example.com/data.txt || {
          echo "Failed to download data" >&2
          exit 1
        }
        
        if [ ! -f data.txt ]; then
          echo "Downloaded file not found" >&2
          exit 1
        fi
        
        process_data data.txt
        generate_report processed_data.txt
      >>>
    }
    """
    
    results = test_linter(ErrorHandlingLinter, good_error_handling_wdl, expected_lint=[])
    print("   ✅ No issues found!")
    
    # Complex workflow structure
    print("\n2. Workflow Structure Issues:")
    complex_workflow_wdl = """
    workflow complex_analysis {
      # Simulate a complex workflow with many elements
      input {
        Array[File] samples
      }
      
      scatter (sample in samples) {
        call task1 { input: file = sample }
        call task2 { input: file = task1.output }
        call task3 { input: file = task2.output }
        call task4 { input: file = task3.output }
        call task5 { input: file = task4.output }
        call task6 { input: file = task5.output }
        call task7 { input: file = task6.output }
        call task8 { input: file = task7.output }
        call task9 { input: file = task8.output }
        call task10 { input: file = task9.output }
      }
      
      call final_analysis { input: files = task10.output }
    }
    """
    
    # Note: This would normally trigger complexity warnings, but we can't test it
    # easily here because the tasks aren't defined. In practice, you'd see:
    print("   Would detect: Workflow complexity issues")
    print("   Recommendation: Break into smaller, modular workflows")


def demo_combined_analysis():
    """Demonstrate running multiple linters on the same code"""
    print("\n🔍 Combined Analysis Demo")
    print("=" * 50)
    
    # WDL with multiple issues
    problematic_wdl = """
    task ProcessSamples {
      input {
        String s
      }
      command {
        rm -rf /tmp/old_data
        cat input.txt | head -10 > output.txt
        mysql -u user -ppassword123 < query.sql
      }
      output {
        File out = "output.txt"
      }
    }
    """
    
    print("\nAnalyzing problematic WDL code with multiple linters:")
    
    # Style issues
    style_results = test_linter(TaskNamingLinter, problematic_wdl, expected_count=1)
    print(f"   Style: {style_results[0][2]}")
    
    # Security issues
    security_results = test_linter(DangerousCommandLinter, problematic_wdl, expected_count=1)
    print(f"   Security: {security_results[0][2]}")
    
    # Performance issues
    perf_results = test_linter(ResourceAllocationLinter, problematic_wdl, expected_count=2)
    print(f"   Performance: {perf_results[0][2]}")
    
    print("\n   This demonstrates how multiple linters can catch different types of issues!")


def main():
    """Run all demonstrations"""
    print("🧪 miniwdl Example Linters Demonstration")
    print("=" * 60)
    print("This demo shows how the example linters can detect various")
    print("issues in WDL code and suggest improvements.")
    
    demo_style_linters()
    demo_security_linters()
    demo_performance_linters()
    demo_best_practices_linters()
    demo_combined_analysis()
    
    print("\n" + "=" * 60)
    print("🎉 Demo completed!")
    print("\nTo use these linters with your own WDL files:")
    print("1. Individual linter:")
    print("   miniwdl check --additional-linters examples/example_linters/style_linters.py:TaskNamingLinter workflow.wdl")
    print("\n2. Multiple linters:")
    print("   miniwdl check --additional-linters \\")
    print("     examples/example_linters/style_linters.py:TaskNamingLinter,\\")
    print("     examples/example_linters/security_linters.py:DangerousCommandLinter \\")
    print("     workflow.wdl")
    print("\n3. Category-based filtering:")
    print("   miniwdl check --enable-lint-categories STYLE,SECURITY workflow.wdl")


if __name__ == "__main__":
    main()

"""
Example security linters for miniwdl

This module contains example linters that detect security vulnerabilities and unsafe practices.
These linters demonstrate best practices for creating security-focused linters.
"""

import re
from WDL.Lint import Linter, LintSeverity, LintCategory


class DangerousCommandLinter(Linter):
    """
    Detects potentially dangerous commands in task command blocks.
    
    This linter flags commands that could be destructive or pose security risks:
    - File system operations (rm, dd, mkfs, fdisk)
    - Privilege escalation (sudo, su)
    - Network operations (wget, curl with suspicious patterns)
    - System modification commands
    
    Examples:
        # Dangerous
        command { rm -rf /data }
        command { sudo apt-get install package }
        command { dd if=/dev/zero of=/dev/sda }
        
        # Better
        command { rm -f output.txt }
        command { apt-get install package }  # if running in container
        command { dd if=input.txt of=output.txt }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MAJOR
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Define dangerous command patterns with their risk levels
        self.dangerous_patterns = {
            # Critical - can destroy data or compromise system
            r'\brm\s+(-[rf]*r[rf]*|-[rf]*f[rf]*)\s+/': {
                'message': 'Recursive rm on root paths is extremely dangerous',
                'severity': LintSeverity.CRITICAL
            },
            r'\bdd\s+if=/dev/': {
                'message': 'Reading from device files can be dangerous',
                'severity': LintSeverity.CRITICAL
            },
            r'\bdd\s+of=/dev/': {
                'message': 'Writing to device files can destroy data',
                'severity': LintSeverity.CRITICAL
            },
            r'\b(mkfs|fdisk|parted)\b': {
                'message': 'Disk partitioning/formatting commands can destroy data',
                'severity': LintSeverity.CRITICAL
            },
            
            # Major - privilege escalation or system modification
            r'\bsudo\b': {
                'message': 'Avoid sudo in containerized tasks - use appropriate base image',
                'severity': LintSeverity.MAJOR
            },
            r'\bsu\s+': {
                'message': 'User switching should be avoided in tasks',
                'severity': LintSeverity.MAJOR
            },
            r'\bchmod\s+777': {
                'message': 'Setting 777 permissions is a security risk',
                'severity': LintSeverity.MAJOR
            },
            
            # Moderate - potentially risky operations
            r'\brm\s+-rf\s+\$': {
                'message': 'Recursive rm with variables can be dangerous if variable is empty',
                'severity': LintSeverity.MODERATE
            },
            r'\bwget\s+.*\|\s*sh': {
                'message': 'Downloading and executing scripts is risky',
                'severity': LintSeverity.MAJOR
            },
            r'\bcurl\s+.*\|\s*sh': {
                'message': 'Downloading and executing scripts is risky',
                'severity': LintSeverity.MAJOR
            },
        }
        
        # Compile patterns for efficiency
        self.compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), info)
            for pattern, info in self.dangerous_patterns.items()
        ]
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        # Check each dangerous pattern
        for pattern, info in self.compiled_patterns:
            if pattern.search(command_str):
                self.add(
                    obj,
                    f"Potentially dangerous command detected: {info['message']}",
                    obj.pos,
                    severity=info['severity']
                )


class CredentialScannerLinter(Linter):
    """
    Scans for hardcoded credentials and sensitive information in commands.
    
    This linter detects patterns that might indicate hardcoded:
    - Passwords
    - API keys
    - Tokens
    - Database connection strings
    - SSH keys
    
    Examples:
        # Bad
        command { mysql -u user -ppassword123 }
        command { curl -H "Authorization: Bearer abc123" }
        command { export API_KEY=secret123 }
        
        # Good
        command { mysql -u user -p~{password} }
        command { curl -H "Authorization: Bearer ~{api_token}" }
        command { export API_KEY=~{api_key} }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.CRITICAL
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Define credential patterns
        self.credential_patterns = [
            # Database passwords
            r'-p["\']?[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{}|;:,.<>?]{3,}["\']?(?!\s*~\{)',
            r'--password[=\s]["\']?[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{}|;:,.<>?]{3,}["\']?(?!\s*~\{)',
            
            # Generic password patterns
            r'password\s*[=:]\s*["\']?[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{}|;:,.<>?]{3,}["\']?(?!\s*~\{)',
            r'passwd\s*[=:]\s*["\']?[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{}|;:,.<>?]{3,}["\']?(?!\s*~\{)',
            
            # API keys and tokens
            r'api[_-]?key\s*[=:]\s*["\']?[a-zA-Z0-9]{10,}["\']?(?!\s*~\{)',
            r'token\s*[=:]\s*["\']?[a-zA-Z0-9]{10,}["\']?(?!\s*~\{)',
            r'secret\s*[=:]\s*["\']?[a-zA-Z0-9]{10,}["\']?(?!\s*~\{)',
            
            # Authorization headers
            r'authorization:\s*bearer\s+[a-zA-Z0-9]{10,}(?!\s*~\{)',
            r'authorization:\s*basic\s+[a-zA-Z0-9+/=]{10,}(?!\s*~\{)',
            
            # Connection strings
            r'://[^:]+:[^@]+@',  # protocol://user:pass@host
            
            # SSH keys (partial detection)
            r'-----BEGIN\s+(RSA\s+)?PRIVATE\s+KEY-----',
        ]
        
        # Compile patterns
        self.compiled_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.credential_patterns
        ]
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        # Check for credential patterns
        for pattern in self.compiled_patterns:
            if pattern.search(command_str):
                self.add(
                    obj,
                    "Potential hardcoded credential detected in command - use WDL variables instead",
                    obj.pos,
                    severity=LintSeverity.CRITICAL
                )
                break  # Only report once per task


class NetworkAccessLinter(Linter):
    """
    Flags tasks that make network requests and checks for security best practices.
    
    This linter identifies:
    - Network commands (wget, curl, nc, ssh, etc.)
    - Insecure protocols (http, ftp, telnet)
    - Suspicious network patterns
    
    Examples:
        # Flagged for review
        command { wget http://example.com/file }  # HTTP instead of HTTPS
        command { curl ftp://server/file }       # Insecure FTP
        command { nc -l 1234 }                   # Network listener
        
        # Better
        command { wget https://example.com/file }
        command { curl sftp://server/file }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.network_commands = {
            'wget': 'Downloads files from the internet',
            'curl': 'Makes HTTP/network requests',
            'nc': 'Network utility (netcat)',
            'netcat': 'Network utility',
            'ssh': 'Secure shell connection',
            'scp': 'Secure copy over network',
            'rsync': 'File synchronization (may use network)',
            'ftp': 'File transfer protocol',
            'sftp': 'Secure file transfer protocol',
            'telnet': 'Insecure remote connection',
        }
        
        self.insecure_protocols = {
            r'\bhttp://': 'HTTP is insecure, consider using HTTPS',
            r'\bftp://': 'FTP is insecure, consider using SFTP',
            r'\btelnet://': 'Telnet is insecure, use SSH instead',
        }
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command).lower()
        
        # Check for network commands
        for cmd, description in self.network_commands.items():
            if f' {cmd} ' in f' {command_str} ' or command_str.startswith(f'{cmd} '):
                severity = LintSeverity.MAJOR if cmd in ['telnet', 'nc', 'netcat'] else LintSeverity.MODERATE
                self.add(
                    obj,
                    f"Task uses network command '{cmd}' ({description}) - ensure network access is intended and secure",
                    obj.pos,
                    severity=severity
                )
        
        # Check for insecure protocols
        for pattern, message in self.insecure_protocols.items():
            if re.search(pattern, command_str):
                self.add(
                    obj,
                    f"Insecure protocol detected: {message}",
                    obj.pos,
                    severity=LintSeverity.MAJOR
                )


class FilePermissionLinter(Linter):
    """
    Checks for insecure file permission operations.
    
    This linter identifies:
    - Overly permissive file permissions (777, 666)
    - World-writable directories
    - Executable permissions on data files
    
    Examples:
        # Insecure
        command { chmod 777 file.txt }
        command { chmod 666 script.sh }
        
        # Better
        command { chmod 644 file.txt }
        command { chmod 755 script.sh }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MODERATE
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.permission_patterns = {
            r'\bchmod\s+777\b': {
                'message': 'chmod 777 grants full permissions to everyone - security risk',
                'severity': LintSeverity.MAJOR
            },
            r'\bchmod\s+666\b': {
                'message': 'chmod 666 makes files world-writable - potential security risk',
                'severity': LintSeverity.MODERATE
            },
            r'\bchmod\s+[0-7]*[2367]\b': {
                'message': 'World-writable permissions detected - review security implications',
                'severity': LintSeverity.MODERATE
            },
        }
        
        self.compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), info)
            for pattern, info in self.permission_patterns.items()
        ]
    
    def task(self, obj):
        if not obj.command:
            return
        
        command_str = str(obj.command)
        
        for pattern, info in self.compiled_patterns:
            if pattern.search(command_str):
                self.add(
                    obj,
                    info['message'],
                    obj.pos,
                    severity=info['severity']
                )


class InputValidationLinter(Linter):
    """
    Checks for proper input validation in task commands.
    
    This linter identifies tasks that:
    - Use file inputs without validation
    - Don't check for required parameters
    - Have potential injection vulnerabilities
    
    Examples:
        # Risky
        command { cat ~{input_file} }
        
        # Better
        command {
            if [ ! -f "~{input_file}" ]; then
                echo "Error: Input file not found" >&2
                exit 1
            fi
            cat ~{input_file}
        }
    """
    
    category = LintCategory.SECURITY
    default_severity = LintSeverity.MODERATE
    
    def task(self, obj):
        if not obj.command or not obj.inputs:
            return
        
        command_str = str(obj.command)
        
        # Check each input
        for input_decl in obj.inputs:
            input_name = input_decl.name
            input_type = str(input_decl.type)
            
            # Check if File inputs are used without validation
            if 'File' in input_type and f"~{{{input_name}}}" in command_str:
                # Look for basic file existence checks
                validation_patterns = [
                    f"[ -f ~{{{input_name}}} ]",
                    f"test -f ~{{{input_name}}}",
                    f"[ -e ~{{{input_name}}} ]",
                    f"test -e ~{{{input_name}}}",
                ]
                
                has_validation = any(pattern in command_str for pattern in validation_patterns)
                
                if not has_validation:
                    self.add(
                        obj,
                        f"File input '{input_name}' is used without validation - consider checking if file exists",
                        obj.pos,
                        severity=LintSeverity.MINOR
                    )
            
            # Check for potential command injection via string inputs
            if 'String' in input_type and f"~{{{input_name}}}" in command_str:
                # Look for direct use in shell commands without quoting
                unquoted_patterns = [
                    f"~{{{input_name}}}[^\"']",  # Not followed by quote
                    f"[^\"']~{{{input_name}}}",  # Not preceded by quote
                ]
                
                for pattern in unquoted_patterns:
                    if re.search(pattern, command_str):
                        self.add(
                            obj,
                            f"String input '{input_name}' may be vulnerable to injection - ensure proper quoting",
                            obj.pos,
                            severity=LintSeverity.MODERATE
                        )
                        break

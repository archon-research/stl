#!/usr/bin/env python3
"""
Terraform to AWS Architecture Diagram Generator

Parses Terraform files and generates an AWS architecture diagram using Mermaid.
Supports visualization in markdown files, HTML, or CLI output.

Usage:
    python3 terraform-diagram-generator.py <terraform_dir> [--output diagram.md] [--format html]
"""

import os
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Set
from dataclasses import dataclass, field
import argparse


@dataclass
class TFResource:
    """Represents a Terraform resource"""
    type: str  # e.g., "aws_ecs_cluster"
    name: str  # e.g., "main"
    properties: Dict = field(default_factory=dict)
    
    def full_name(self) -> str:
        return f"{self.type}.{self.name}"
    
    def aws_service(self) -> str:
        """Extract AWS service from resource type"""
        # aws_ecs_cluster -> ecs
        parts = self.type.split('_')
        if parts[0] == 'aws':
            return parts[1]
        return self.type


@dataclass
class TFModule:
    """Represents a Terraform module with resources and connections"""
    resources: List[TFResource] = field(default_factory=list)
    variables: Dict = field(default_factory=dict)
    outputs: Dict = field(default_factory=dict)
    
    def add_resource(self, resource: TFResource):
        self.resources.append(resource)
    
    def get_resources_by_service(self) -> Dict[str, List[TFResource]]:
        """Group resources by AWS service"""
        grouped = {}
        for resource in self.resources:
            service = resource.aws_service()
            if service not in grouped:
                grouped[service] = []
            grouped[service].append(resource)
        return grouped


class TerraformParser:
    """Parse Terraform HCL files"""
    
    def __init__(self, terraform_dir: str):
        self.terraform_dir = Path(terraform_dir)
        self.module = TFModule()
        
    def parse(self) -> TFModule:
        """Parse all .tf files in the directory"""
        tf_files = list(self.terraform_dir.glob('*.tf'))
        
        if not tf_files:
            print(f"Warning: No .tf files found in {self.terraform_dir}")
            return self.module
        
        for tf_file in sorted(tf_files):
            self._parse_file(tf_file)
        
        return self.module
    
    def _parse_file(self, file_path: Path):
        """Parse a single .tf file"""
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Extract resource blocks: resource "TYPE" "NAME" { ... }
        resource_pattern = r'resource\s+"([^"]+)"\s+"([^"]+)"\s*\{([^}]*(?:\{[^}]*\}[^}]*)*)\}'
        
        for match in re.finditer(resource_pattern, content, re.DOTALL):
            resource_type = match.group(1)
            resource_name = match.group(2)
            resource_body = match.group(3)
            
            properties = self._parse_properties(resource_body)
            resource = TFResource(resource_type, resource_name, properties)
            self.module.add_resource(resource)
    
    def _parse_properties(self, body: str) -> Dict:
        """Extract key properties from resource body"""
        properties = {}
        
        # Simple key-value extraction
        patterns = {
            'name': r'name\s*=\s*(?:"([^"]+)"|([a-zA-Z0-9_.-]+))',
            'image': r'image\s*=\s*"([^"]+)"',
            'engine': r'engine\s*=\s*"([^"]+)"',
            'node_type': r'node_type\s*=\s*"([^"]+)"',
            'instance_type': r'instance_type\s*=\s*"([^"]+)"',
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, body)
            if match:
                properties[key] = match.group(1) if match.group(1) else match.group(2)
        
        return properties


class MermaidDiagramGenerator:
    """Generate Mermaid diagrams from Terraform module"""
    
    # AWS service icons and colors
    SERVICE_CONFIG = {
        'ec2': {'icon': '🖥️', 'color': '#FF9900'},
        'ecs': {'icon': '📦', 'color': '#FF9900'},
        'elb': {'icon': '⚖️', 'color': '#FF9900'},
        'elasticache': {'icon': '⚡', 'color': '#EC7211'},
        'rds': {'icon': '🗄️', 'color': '#527FFF'},
        's3': {'icon': '📦', 'color': '#569A31'},
        'sns': {'icon': '📢', 'color': '#FF9900'},
        'sqs': {'icon': '📋', 'color': '#FF9900'},
        'vpc': {'icon': '🌐', 'color': '#4B9BFF'},
        'iam': {'icon': '🔐', 'color': '#DD344C'},
        'secretsmanager': {'icon': '🔑', 'color': '#DD344C'},
        'cloudwatch': {'icon': '📊', 'color': '#759C3E'},
        'lambda': {'icon': '⚡', 'color': '#FF9900'},
        'ecr': {'icon': '📦', 'color': '#FF9900'},
    }
    
    def __init__(self, module: TFModule):
        self.module = module
        self.resource_map = {}  # Map resource full_name to mermaid node id
        
    def generate(self) -> str:
        """Generate Mermaid diagram"""
        lines = ['graph TB']
        lines.append('    classDef awsService fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#fff')
        lines.append('    classDef network fill:#4B9BFF,stroke:#232F3E,stroke-width:2px,color:#fff')
        lines.append('    classDef database fill:#527FFF,stroke:#232F3E,stroke-width:2px,color:#fff')
        lines.append('    classDef security fill:#DD344C,stroke:#232F3E,stroke-width:2px,color:#fff')
        lines.append('    classDef storage fill:#569A31,stroke:#232F3E,stroke-width:2px,color:#fff')
        lines.append('')
        
        # Group resources by service
        services = self.module.get_resources_by_service()
        
        # Create subgraphs for services
        for service, resources in sorted(services.items()):
            lines.append(f'    subgraph {service.upper()}["AWS {service.upper()}"]')
            for resource in resources:
                node_id = self._get_node_id(resource)
                label = self._get_node_label(resource)
                lines.append(f'        {node_id}["{label}"]')
            lines.append('    end')
            lines.append('')
        
        # Add connections based on resource dependencies
        connections = self._extract_connections()
        for source, target, label in connections:
            lines.append(f'    {source} -->|{label}| {target}')
        
        # Apply styling
        for service in services.keys():
            node_ids = [self._get_node_id(r) for r in services[service]]
            style = self._get_service_style(service)
            lines.append(f'    class {",".join(node_ids)} {style}')
        
        return '\n'.join(lines)
    
    def _get_node_id(self, resource: TFResource) -> str:
        """Generate unique node ID for mermaid"""
        node_id = f"{resource.type}_{resource.name}".replace('-', '_').replace('.', '_')
        self.resource_map[resource.full_name()] = node_id
        return node_id
    
    def _get_node_label(self, resource: TFResource) -> str:
        """Generate display label for resource"""
        # Use custom name if available
        if 'name' in resource.properties:
            display_name = resource.properties['name']
        else:
            display_name = resource.name
        
        # Get icon
        service = resource.aws_service()
        icon = self.SERVICE_CONFIG.get(service, {}).get('icon', '🔧')
        
        # Format: icon + resource type + name
        return f"{icon} {service}<br/>{display_name}"
    
    def _extract_connections(self) -> List[Tuple[str, str, str]]:
        """Extract resource dependencies"""
        connections = []
        
        # Common connection patterns
        dependency_patterns = {
            'aws_ecs_service': [
                ('cluster_arn', 'aws_ecs_cluster', 'uses cluster'),
                ('load_balancer_config', 'aws_lb', 'registers with'),
            ],
            'aws_ecs_task_definition': [
                ('container_definitions', 'aws_ecr_repository', 'uses image'),
            ],
            'aws_elasticache_cluster': [
                ('security_group_ids', 'aws_security_group', 'uses SG'),
            ],
            'aws_db_instance': [
                ('db_subnet_group_name', 'aws_db_subnet_group', 'in subnet group'),
            ],
        }
        
        for resource in self.module.resources:
            patterns = dependency_patterns.get(resource.type, [])
            
            for prop_name, target_type, label in patterns:
                if prop_name in resource.properties:
                    source_id = self._get_node_id(resource)
                    # Try to find matching target
                    for target in self.module.resources:
                        if target.type == target_type:
                            target_id = self._get_node_id(target)
                            connections.append((source_id, target_id, label))
        
        return connections
    
    def _get_service_style(self, service: str) -> str:
        """Get mermaid style class for service"""
        style_map = {
            'vpc': 'network',
            'iam': 'security',
            'secretsmanager': 'security',
            's3': 'storage',
            'rds': 'database',
            'elasticache': 'database',
        }
        return style_map.get(service, 'awsService')


class DiagramExporter:
    """Export diagrams in various formats"""
    
    @staticmethod
    def to_markdown(mermaid_diagram: str, title: str = "AWS Architecture Diagram") -> str:
        """Export as Markdown with embedded Mermaid"""
        return f"""# {title}

Generated from Terraform configuration.

```mermaid
{mermaid_diagram}
```

## Resources Found

- VPC: Networking infrastructure
- ECS: Container orchestration
- ElastiCache: In-memory caching
- RDS/TigerData: Database layer
- S3: Object storage
- SNS/SQS: Messaging
- Secrets Manager: Secrets storage
- IAM: Access control
- CloudWatch: Monitoring
"""
    
    @staticmethod
    def to_html(mermaid_diagram: str, title: str = "AWS Architecture Diagram") -> str:
        """Export as HTML with Mermaid rendering"""
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #232F3E;
            margin-top: 0;
        }}
        .mermaid {{
            display: flex;
            justify-content: center;
            margin: 20px 0;
        }}
        .note {{
            background: #f0f4f8;
            border-left: 4px solid #FF9900;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <div class="note">
            <strong>Note:</strong> This diagram was automatically generated from Terraform configuration files.
        </div>
        <div class="mermaid">
{mermaid_diagram}
        </div>
    </div>
    <script>
        mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
        mermaid.contentLoaded();
    </script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(
        description='Generate AWS architecture diagrams from Terraform files'
    )
    parser.add_argument('terraform_dir', help='Path to Terraform directory')
    parser.add_argument(
        '--output',
        default='terraform-architecture.md',
        help='Output file path (default: terraform-architecture.md)'
    )
    parser.add_argument(
        '--format',
        choices=['markdown', 'html', 'mermaid'],
        default='markdown',
        help='Output format (default: markdown)'
    )
    parser.add_argument(
        '--title',
        default='AWS Architecture Diagram',
        help='Diagram title'
    )
    
    args = parser.parse_args()
    
    # Validate terraform directory
    tf_dir = Path(args.terraform_dir)
    if not tf_dir.exists():
        print(f"Error: Directory not found: {tf_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Parse Terraform files
    print(f"Parsing Terraform files from {tf_dir}...")
    tf_parser = TerraformParser(str(tf_dir))
    module = tf_parser.parse()
    
    print(f"Found {len(module.resources)} resources")
    
    # Generate diagram
    print("Generating Mermaid diagram...")
    generator = MermaidDiagramGenerator(module)
    mermaid_diagram = generator.generate()
    
    # Export
    if args.format == 'markdown':
        output = DiagramExporter.to_markdown(mermaid_diagram, args.title)
    elif args.format == 'html':
        output = DiagramExporter.to_html(mermaid_diagram, args.title)
    else:  # mermaid
        output = mermaid_diagram
    
    # Write output
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        f.write(output)
    
    print(f"✓ Diagram generated: {output_path}")
    print(f"\nResources by service:")
    for service, resources in sorted(module.get_resources_by_service().items()):
        print(f"  {service}: {len(resources)} resources")


if __name__ == '__main__':
    main()

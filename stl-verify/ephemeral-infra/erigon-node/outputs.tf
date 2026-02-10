output "vpc_id" {
  description = "VPC ID"
  value       = data.aws_vpc.main.id
}

output "subnet_id" {
  description = "Subnet ID"
  value       = data.aws_subnet.private.id
}

output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.erigon_c8gd.id
}

output "private_ip" {
  description = "Private IP address"
  value       = aws_instance.erigon_c8gd.private_ip
}

output "tailscale_access" {
  description = "Access via Tailscale SSH"
  value       = "tailscale ssh root@<tailscale-ip> (check Tailscale admin console for IP, typically 100.x.x.x)"
}

output "rpc_endpoint_tailscale" {
  description = "Erigon RPC endpoint (via Tailscale)"
  value       = "http://<tailscale-ip>:8545"
}

output "sync_status_command" {
  description = "Command to check sync status (run on instance)"
  value       = "curl -s localhost:8545 -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_syncing\",\"params\":[],\"id\":1}'"
}

output "s3_bucket" {
  description = "S3 bucket for data export"
  value       = var.s3_bucket_name
}

output "s3_test_command" {
  description = "Command to test S3 access from the instance"
  value       = "aws s3 ls s3://${var.s3_bucket_name}/ --region ${var.aws_region}"
}

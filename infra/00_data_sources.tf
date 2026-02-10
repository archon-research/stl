# Reference secrets created by bootstrap
# These are managed by bootstrap and just looked up here

data "aws_secretsmanager_secret" "watcher_config" {
  name = "stl-${var.environment}-watcher-config"
}

data "aws_secretsmanager_secret" "tigerdata" {
  name = "stl-${var.environment}-tigerdata"
}

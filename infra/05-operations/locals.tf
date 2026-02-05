locals {
  prefix                = var.prefix
  prefix_lowercase      = lower(local.prefix)
  tigerdata_vpc_cidr    = "10.0.0.0/16"
}

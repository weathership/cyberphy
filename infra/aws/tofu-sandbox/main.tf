# =============================================================================
# THROWAWAY single-node air-gap CONVERGE-VALIDATION sandbox.
#
# ISOLATED BY CONSTRUCTION: this is a SEPARATE tofu root with its OWN local state
# (infra/aws/tofu-sandbox/terraform.tfstate). It shares NOTHING with the live
# cluster's state (infra/aws/tofu/terraform.tfstate) — a `tofu apply`/`destroy`
# here can never touch the live 8-worker cluster or the 885 GB S3 bucket.
#
# It stands up ONE amd64 RKE2 node (single-node, schedulable control plane, NO
# default StorageClass — the bare baseline) in the DEFAULT VPC, so we can transport
# the release package, inject the difficult starting conditions (image-purged,
# leftover default SC, leftover/oversubscribed workloads), cut egress to simulate
# the closed world, and prove `converge-node.sh apply` reaches green from one command.
#
# Auth: honors the ambient AWS_PROFILE (the active SSO session). A pre-apply STS
# account assertion (see run-sandbox.sh) guards against the wrong account.
# Teardown: `tofu destroy` — destroys ONLY these resources.
# =============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    tls    = { source = "hashicorp/tls", version = "~> 4.0" }
    local  = { source = "hashicorp/local", version = "~> 2.0" }
    random = { source = "hashicorp/random", version = "~> 3.0" }
  }
}

provider "aws" {
  region = var.aws_region
  # No hardcoded profile: honor the ambient AWS_PROFILE / SDK default chain (the
  # active SSO session that `aws sts get-caller-identity` already resolved to
  # 050330818249). run-sandbox.sh asserts the account before any apply.
  default_tags { tags = local.tags }
}

# -----------------------------------------------------------------------------
variable "aws_region" { default = "us-east-1" }
variable "instance_type" { default = "m7i.2xlarge" } # 8 vCPU / 32 GiB — whole stack on one node
variable "root_gb" { default = 80 }                  # package + registry + minio + images, with headroom
variable "ssh_cidr" { description = "your egress /32 for SSH (e.g. 1.2.3.4/32)" }
variable "extra_ssh_cidrs" {
  description = "additional operator CIDRs (e.g. a CGNAT carrier's ranges when the egress IP rotates mid-session — a rotation severed two live validation runs)"
  type        = list(string)
  default     = []
}
variable "key_path" {
  description = "where to write the generated SSH private key; the harness points this into build/sandbox (gitignored)"
  default     = ""
}
variable "allow_egress" {
  description = "true during bootstrap (pull RKE2/zarf/minio); set false to SIMULATE AIR-GAP before converge"
  default     = true
}

locals {
  # Key lives beside main.tf for standalone use; the harness overrides it into
  # build/sandbox (gitignored) via -var key_path=... so no secret lands in the tree.
  key_path = var.key_path != "" ? var.key_path : "${path.module}/sandbox.pem"
  tags = {
    Project   = "cybersec-sandbox"
    Purpose   = "airgap-converge-validation"
    Owner     = "rhill"
    Ephemeral = "true"
  }
  # Minimal bootstrap: single-node RKE2 + zarf + python3. Everything else (package
  # transport, minio image pre-pull, fault injection, converge) is driven over SSH
  # so it's observable and ordered relative to the egress cut.
  user_data = <<-EOF
    #!/bin/bash
    exec > /var/log/sandbox-bootstrap.log 2>&1
    set -ux
    echo "STATUS=installing-rke2" > /var/tmp/bootstrap-status
    # tar method: installs from GitHub releases — immune to the rpm.rancher.io
    # repo-layout 404 that broke the RPM path (observed 2026-07-30:
    # rke2/stable/stable/centos/8 repomd.xml -> 404, no rke2-server unit).
    # The RPM path also pulled OS deps; tar does not — AL2023 ships NO iptables
    # userland, and without it the portmap CNI fails AFTER calico allocates an
    # IP, leaking one per retry until the /24 exhausts (observed: x360 retries,
    # then "no IP addresses available in range set: 10.42.0.1-10.42.0.254").
    dnf install -y iptables-nft
    curl -sfL https://get.rke2.io | INSTALL_RKE2_METHOD=tar sh -
    systemctl enable --now rke2-server.service
    for i in $(seq 1 60); do [ -f /etc/rancher/rke2/rke2.yaml ] && break; sleep 5; done
    ln -sf /var/lib/rancher/rke2/bin/kubectl /usr/local/bin/kubectl
    echo "STATUS=installing-zarf" > /var/tmp/bootstrap-status
    curl -sL https://github.com/zarf-dev/zarf/releases/download/v0.70.1/zarf_v0.70.1_Linux_amd64 -o /usr/local/bin/zarf
    chmod +x /usr/local/bin/zarf
    dnf install -y python3 >/dev/null 2>&1 || true
    echo "STATUS=ready" > /var/tmp/bootstrap-status
  EOF
}

# -----------------------------------------------------------------------------
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}
data "aws_vpc" "default" { default = true }
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "random_id" "suffix" { byte_length = 3 }

resource "tls_private_key" "sandbox" {
  algorithm = "RSA"
  rsa_bits  = 4096
}
resource "aws_key_pair" "sandbox" {
  key_name   = "cybersec-sandbox-${random_id.suffix.hex}"
  public_key = tls_private_key.sandbox.public_key_openssh
}
resource "local_sensitive_file" "pem" {
  content         = tls_private_key.sandbox.private_key_pem
  filename        = local.key_path
  file_permission = "0600"
}

resource "aws_security_group" "sandbox" {
  name        = "cybersec-sandbox-${random_id.suffix.hex}"
  description = "throwaway single-node airgap converge sandbox"
  vpc_id      = data.aws_vpc.default.id
}
resource "aws_security_group_rule" "ssh_in" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = concat([var.ssh_cidr], var.extra_ssh_cidrs)
  security_group_id = aws_security_group.sandbox.id
  description       = "SSH from operator /32 (stateful: survives the egress cut)"
}
# Egress is the ONLY thing the air-gap toggle controls. SGs are stateful, so removing
# this does NOT drop the inbound SSH session — it only stops the node INITIATING
# outbound (image pulls / internet). `tofu apply -var allow_egress=false` = closed world.
resource "aws_security_group_rule" "egress_all" {
  count             = var.allow_egress ? 1 : 0
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.sandbox.id
}

resource "aws_instance" "sandbox" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.sandbox.key_name
  vpc_security_group_ids      = [aws_security_group.sandbox.id]
  user_data                   = local.user_data

  root_block_device {
    volume_size = var.root_gb
    volume_type = "gp3"
  }
  metadata_options {
    http_tokens = "required" # IMDSv2
  }
  tags = { Name = "cybersec-sandbox" }

  # The SSM "latest AL2023" lookup re-resolves as Amazon publishes new AMIs — without
  # this, a config-only re-apply (e.g. refreshing ssh_cidr after the operator's
  # residential IP rotates) REPLACES the node and destroys the in-flight validation
  # state (it did: a /32 refresh terminated a mid-matrix node). Fresh provisions still
  # get the latest AMI; an existing node is never replaced by AMI drift.
  lifecycle {
    ignore_changes = [ami]
  }
}

# -----------------------------------------------------------------------------
output "public_ip" { value = aws_instance.sandbox.public_ip }
output "instance_id" { value = aws_instance.sandbox.id }
output "ssh_key" { value = local_sensitive_file.pem.filename }
output "ssh_cmd" {
  value = "ssh -i ${local_sensitive_file.pem.filename} -o StrictHostKeyChecking=no ec2-user@${aws_instance.sandbox.public_ip}"
}

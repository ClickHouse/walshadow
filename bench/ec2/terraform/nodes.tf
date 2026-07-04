locals {
  all_nodes = {
    source-pg = {
      dir     = "ec2-source-pg"
      name    = "walshadow-source-pg"
      sg_desc = "walshadow source postgres: SSH+PG from my IP and VPC"
      ingress = [
        { port = 22, scopes = ["my", "vpc"] },
        { port = 5432, scopes = ["my", "vpc"] },
      ]
      # extra state.env key carrying the private IP, read by deploys + bench
      aliases = ["SOURCE_PRIVATE_IP"]
      enabled = true
    }

    # 8123 HTTP, 9000 native
    clickhouse = {
      dir     = "ec2-clickhouse"
      name    = "walshadow-clickhouse"
      sg_desc = "walshadow clickhouse: SSH from my IP; HTTP/native from my IP + VPC"
      ingress = [
        { port = 22, scopes = ["my"] },
        { port = 8123, scopes = ["my", "vpc"] },
        { port = 9000, scopes = ["my", "vpc"] },
      ]
      aliases = []
      enabled = var.clickhouse
    }

    # 9484 metrics, 3000 grafana, 9090 prometheus, 16686 jaeger
    walshadow = {
      dir     = "ec2-walshadow"
      name    = "walshadow-daemon"
      sg_desc = "walshadow daemon: SSH + metrics + grafana/prometheus/jaeger from my IP"
      ingress = [
        { port = 22, scopes = ["my"] },
        { port = 9484, scopes = ["my"] },
        { port = 3000, scopes = ["my"] },
        { port = 9090, scopes = ["my"] },
        { port = 16686, scopes = ["my"] },
      ]
      aliases = []
      enabled = var.streamer == "walshadow"
    }

    # 3000 UI, 9900 SQL, 9001 MinIO (ClickHouse loads staged avro via s3())
    peerdb = {
      dir     = "ec2-peerdb"
      name    = "walshadow-peerdb"
      sg_desc = "walshadow peerdb: SSH + PeerDB UI/SQL from my IP; MinIO from VPC"
      ingress = [
        { port = 22, scopes = ["my"] },
        { port = 3000, scopes = ["my"] },
        { port = 9900, scopes = ["my"] },
        { port = 9001, scopes = ["vpc"] },
      ]
      aliases = []
      enabled = var.streamer == "peerdb"
    }

    pg-standby = {
      dir     = "ec2-pg-standby"
      name    = "walshadow-pg-standby"
      sg_desc = "walshadow pg standby: SSH from my IP; PG from my IP + VPC"
      ingress = [
        { port = 22, scopes = ["my"] },
        { port = 5432, scopes = ["my", "vpc"] },
      ]
      aliases = []
      enabled = var.streamer == "pg"
    }

    bench = {
      dir     = "ec2-bench"
      name    = "walshadow-bench"
      sg_desc = "walshadow bench runner: SSH from my IP"
      ingress = [
        { port = 22, scopes = ["my"] },
      ]
      aliases = []
      enabled = var.bench_runner
    }
  }

  nodes = { for k, n in local.all_nodes : k => n if n.enabled }
}

# Latest Ubuntu 24.04 LTS amd64 (Canonical, owner 099720109477).
data "aws_ami" "ubuntu_noble" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

resource "tls_private_key" "bench" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "bench" {
  key_name   = "walshadow-bench"
  public_key = tls_private_key.bench.public_key_openssh
}

resource "local_sensitive_file" "pem" {
  content         = tls_private_key.bench.private_key_openssh
  filename        = "${path.module}/walshadow-bench.pem"
  file_permission = "0600"
}

resource "aws_instance" "node" {
  for_each = local.nodes

  # egress is a separate rule resource; booting without it breaks cloud-init
  # downloads (apt, docker pulls)
  depends_on = [aws_vpc_security_group_egress_rule.node]

  ami                         = data.aws_ami.ubuntu_noble.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.bench.id
  vpc_security_group_ids      = [aws_security_group.node[each.key].id]
  key_name                    = aws_key_pair.bench.key_name
  user_data                   = file("${path.module}/../${each.value.dir}/cloud-init.yaml")
  user_data_replace_on_change = true

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
  }

  tags = {
    Name = each.value.name
  }
}

# read by deploy.sh / profile.sh / walshadow-ec2-bench
resource "local_file" "state_env" {
  for_each = local.nodes

  filename        = "${path.module}/../${each.value.dir}/state.env"
  file_permission = "0644"
  content = join("\n", concat(
    [
      "INSTANCE_ID=${aws_instance.node[each.key].id}",
      "PUBLIC_IP=${aws_instance.node[each.key].public_ip}",
      "PRIVATE_IP=${aws_instance.node[each.key].private_ip}",
    ],
    [for a in each.value.aliases : "${a}=${aws_instance.node[each.key].private_ip}"],
    [
      "SG_ID=${aws_security_group.node[each.key].id}",
      "KEY_NAME=${aws_key_pair.bench.key_name}",
      "PEM=../terraform/walshadow-bench.pem",
      "REGION=${var.region}",
      "",
    ],
  ))
}

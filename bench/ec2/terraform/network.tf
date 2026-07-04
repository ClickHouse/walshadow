data "http" "my_ip" {
  count = var.my_ip == "" ? 1 : 0
  url   = "https://checkip.amazonaws.com"
}

# sorted at use: AWS does not guarantee a stable order
data "aws_ec2_instance_type_offerings" "itype" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

locals {
  my_ip_cidr = var.my_ip != "" ? var.my_ip : "${trimspace(data.http.my_ip[0].response_body)}/32"
  az         = var.az != "" ? var.az : sort(data.aws_ec2_instance_type_offerings.itype.locations)[0]
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "walshadow-bench" }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
  tags   = { Name = "walshadow-bench" }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = { Name = "walshadow-bench-public" }
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }

  tags = { Name = "walshadow-bench" }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "node" {
  for_each = local.nodes

  name        = each.value.name
  description = each.value.sg_desc
  vpc_id      = aws_vpc.bench.id

  tags = { Name = each.value.name }
}

# scope is my (operator /32), vpc (VPC CIDR), or a literal CIDR
locals {
  ingress_rules = {
    for r in flatten([
      for nk, n in local.nodes : [
        for i in n.ingress : [
          for s in i.scopes : { node = nk, port = i.port, scope = s }
        ]
      ]
    ]) : "${r.node}-${r.port}-${r.scope}" => r
  }
}

resource "aws_vpc_security_group_ingress_rule" "node" {
  for_each = local.ingress_rules

  security_group_id = aws_security_group.node[each.value.node].id
  description       = "${each.value.port} from ${each.value.scope}"
  cidr_ipv4 = (
    each.value.scope == "my" ? local.my_ip_cidr :
    each.value.scope == "vpc" ? aws_vpc.bench.cidr_block :
    each.value.scope
  )
  from_port   = each.value.port
  to_port     = each.value.port
  ip_protocol = "tcp"
}

# aws_security_group revokes AWS's default allow-all egress on create
resource "aws_vpc_security_group_egress_rule" "node" {
  for_each = local.nodes

  security_group_id = aws_security_group.node[each.key].id
  description       = "all egress (apt, docker pulls, VPC peers)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

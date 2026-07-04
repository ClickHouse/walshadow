output "az" {
  value = local.az
}

output "nodes" {
  value = {
    for k, n in local.nodes : k => {
      name       = n.name
      public_ip  = aws_instance.node[k].public_ip
      private_ip = aws_instance.node[k].private_ip
    }
  }
}

output "ssh_pem" {
  value = local_sensitive_file.pem.filename
}

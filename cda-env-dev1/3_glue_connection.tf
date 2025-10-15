# Create security group for glue connection
data "aws_subnet" "private" {
  for_each = toset(var.private_subnet_ids)
  id       = each.value
}

locals {
  private_subnet_ids_az_map = { for subnet in data.aws_subnet.private : subnet.id => subnet.availability_zone }
  vpc_id                    = data.aws_subnet.private[var.private_subnet_ids[0]].vpc_id
}

data "http" "okta_ip_ranges" {
  url = "https://s3.amazonaws.com/okta-ip-ranges/ip_ranges.json"

  # Optional request headers
  request_headers = {
    Accept = "application/json"
  }

  lifecycle {
    postcondition {
      condition     = contains([200, 201, 204], self.status_code)
      error_message = "Status code invalid"
    }
  }
}

locals {
  okta_ip_ranges = jsondecode(data.http.okta_ip_ranges.response_body)
}

# region us_cell_1

resource "aws_ec2_managed_prefix_list" "okta_us_cell_1_1" {
  name           = "The first set of 100 Okta CIDR blocks in us_cell_1"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(slice(
      sort(local.okta_ip_ranges.us_cell_1.ip_ranges),
      0,
      100
    ))

    content {
      cidr = entry.key
    }
  }
}

resource "aws_ec2_managed_prefix_list" "okta_us_cell_1_2" {
  name           = "The second set of 100 Okta CIDR blocks in us_cell_1"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(slice(
      sort(local.okta_ip_ranges.us_cell_1.ip_ranges),
      100,
      200
    ))

    content {
      cidr = entry.key
    }
  }
}

resource "aws_ec2_managed_prefix_list" "okta_us_cell_1_3" {
  name           = "The third set of 100 Okta CIDR blocks in us_cell_1"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(slice(
      sort(local.okta_ip_ranges.us_cell_1.ip_ranges),
      200,
      300
    ))

    content {
      cidr = entry.key
    }
  }
}
resource "aws_ec2_managed_prefix_list" "okta_us_cell_1_4" {
  name           = "The fourth set of 100 Okta CIDR blocks in us_cell_1"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(slice(
      sort(local.okta_ip_ranges.us_cell_1.ip_ranges),
      300,
      length(local.okta_ip_ranges.us_cell_1.ip_ranges)
    ))

    content {
      cidr = entry.key
    }
  }
}

# endregion us_cell_1

# region us_cell_14

resource "aws_ec2_managed_prefix_list" "okta_us_cell_14" {
  name           = "All Okta CIDR blocks in us_cell_14"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(sort(local.okta_ip_ranges.us_cell_14.ip_ranges))

    content {
      cidr = entry.key
    }
  }
}

# endregion us_cell_14

# region preview_cell_1

resource "aws_ec2_managed_prefix_list" "okta_preview_cell_1" {
  name           = "All Okta CIDR blocks in preview_cell_1"
  address_family = "IPv4"
  max_entries    = 100

  lifecycle {
    create_before_destroy = false
  }

  dynamic "entry" {
    for_each = toset(sort(local.okta_ip_ranges.preview_cell_1.ip_ranges))

    content {
      cidr = entry.key
    }
  }
}

# endregion preview_cell_1

resource "aws_security_group" "default" {
  name_prefix = "aws-use1-${var.environment}-glue-connection-sg-default"
  description = "Glue Connection Security Group"
  vpc_id      = local.vpc_id

  ingress {
    description = "HTTPS Ingress"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  ingress {
    description = "Self-Ingress for all ports."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  dynamic "egress" {
    for_each = var.ms_sql_edw_sever_ports
    content {
      description = "On-Prem EDW SQL Egress"
      from_port   = egress.value
      to_port     = egress.value
      protocol    = "tcp"
      cidr_blocks = var.ms_sql_edw_sever_cidr_blocks
    }
  }

  egress {
    description = "HTTPS Egress"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    description = "HTTP Egress"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    description = "Self-egress for all ports."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    description     = "HTTPS Egress for S3 Service Prefix List"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [var.prefix_list_id]
  }

  egress {
    description = "Postgres Egress for RDS Cluster"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.rds_cluster_cidr_blocks
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "okta_us_cell_1_1" {
  name_prefix = "aws-use1-${var.environment}-glue-connection-sg-okta-us-cell-1-1"
  description = "Glue Connection Security Group for the first set of 200 Okta Egress rules related to us_cell_1"
  vpc_id      = local.vpc_id

  egress {
    description = "Okta Egress"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    prefix_list_ids = [
      aws_ec2_managed_prefix_list.okta_us_cell_1_1.id,
      aws_ec2_managed_prefix_list.okta_us_cell_1_2.id
    ]
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_ec2_managed_prefix_list.okta_us_cell_1_1,
    aws_ec2_managed_prefix_list.okta_us_cell_1_2
  ]
}

resource "aws_security_group" "okta_us_cell_1_2" {
  name_prefix = "aws-use1-${var.environment}-glue-connection-sg-okta-us-cell-1-2"
  description = "Glue Connection Security Group for the second set of 200 Okta Egress rules related to us_cell_1"
  vpc_id      = local.vpc_id

  egress {
    description = "Okta Egress"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    prefix_list_ids = [
      aws_ec2_managed_prefix_list.okta_us_cell_1_3.id,
      aws_ec2_managed_prefix_list.okta_us_cell_1_4.id
    ]
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_ec2_managed_prefix_list.okta_us_cell_1_3,
    aws_ec2_managed_prefix_list.okta_us_cell_1_4
  ]
}

resource "aws_security_group" "okta_us_cell_14" {
  name_prefix = "aws-use1-${var.environment}-glue-connection-sg-okta-us-cell-14"
  description = "Glue Connection Security Group for Okta Egress rules related to us_cell_14"
  vpc_id      = local.vpc_id

  egress {
    description     = "Okta Egress"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [aws_ec2_managed_prefix_list.okta_us_cell_14.id]
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_ec2_managed_prefix_list.okta_us_cell_14
  ]
}

resource "aws_security_group" "okta_preview_cell_1" {
  name_prefix = "aws-use1-${var.environment}-glue-connection-sg-okta-preview-cell-1"
  description = "Glue Connection Security Group for Okta Egress rules related to preview_cell_1"
  vpc_id      = local.vpc_id

  egress {
    description     = "Okta Egress"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [aws_ec2_managed_prefix_list.okta_preview_cell_1.id]
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_ec2_managed_prefix_list.okta_preview_cell_1
  ]
}

#Create glue connection

resource "aws_glue_connection" "default" {
  for_each        = local.private_subnet_ids_az_map
  name            = "aws-use1-${var.environment}-glue-network-connection-${each.key}"
  description     = "Glue Job network connection for subnet: ${each.key}"
  connection_type = "NETWORK"

  connection_properties = {}

  lifecycle {
    create_before_destroy = false
  }

  physical_connection_requirements {
    availability_zone = each.value
    security_group_id_list = [
      aws_security_group.default.id,
      aws_security_group.okta_us_cell_1_1.id,
      aws_security_group.okta_us_cell_1_2.id,
      aws_security_group.okta_us_cell_14.id,
      aws_security_group.okta_preview_cell_1.id
    ]
    subnet_id = each.key
  }

  depends_on = [
    aws_ec2_managed_prefix_list.okta_us_cell_1_1,
    aws_ec2_managed_prefix_list.okta_us_cell_1_2,
    aws_ec2_managed_prefix_list.okta_us_cell_14,
    aws_ec2_managed_prefix_list.okta_preview_cell_1
  ]
}

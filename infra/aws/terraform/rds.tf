resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "main" {
  identifier     = "${var.project_name}-${var.environment}"
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage      = 20
  storage_type           = "gp3"
  db_name                = var.db_name
  username               = var.db_username
  password               = random_password.db.result
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az                = !var.demo_mode
  deletion_protection     = !var.demo_mode
  skip_final_snapshot     = var.demo_mode
  backup_retention_period = var.demo_mode ? 1 : 7
  publicly_accessible     = false

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

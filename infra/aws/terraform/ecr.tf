# One repository shared by all five services — same image, different container
# command per ECS task definition, exactly mirroring docker-compose.yml locally.
resource "aws_ecr_repository" "chronos" {
  name                 = "${var.project_name}-${var.environment}"
  image_tag_mutability = "IMMUTABLE" # a tag always points at the same build — no silent redeploys

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "chronos" {
  repository = aws_ecr_repository.chronos.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "expire untagged images after 7 days"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 7
      }
      action = { type = "expire" }
    }]
  })
}

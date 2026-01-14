# IAM Role for read+write access to the alchemy-ethereum-raw S3 bucket

# Trust policy - services that can assume this role
data "aws_iam_policy_document" "s3_access_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# IAM Role
resource "aws_iam_role" "ethereum_raw_data_access" {
  name               = "stl-ethereum-raw-data-access"
  assume_role_policy = data.aws_iam_policy_document.s3_access_assume_role.json
}

# Policy granting read+write access to the S3 bucket
data "aws_iam_policy_document" "s3_read_write" {
  # List bucket contents
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.main.arn]
  }

  # Read objects
  statement {
    sid    = "ReadObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetObjectTagging"
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }

  # Write objects
  statement {
    sid    = "WriteObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion"
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }
}

resource "aws_iam_policy" "ethereum_raw_data_access" {
  name        = "stl-ethereum-raw-data-access"
  description = "Read and write access to alchemy-ethereum-raw S3 bucket"
  policy      = data.aws_iam_policy_document.s3_read_write.json
}

resource "aws_iam_role_policy_attachment" "ethereum_raw_data_access" {
  role       = aws_iam_role.ethereum_raw_data_access.name
  policy_arn = aws_iam_policy.ethereum_raw_data_access.arn
}

resource "aws_iam_role" "dbt-duckdb" {
  name               = "dbt-duckdb-webinar-${var.env_name}"
  assume_role_policy = data.aws_iam_policy_document.dbt_duckdb_assume_role.json
}

data "aws_iam_policy_document" "dbt_duckdb_assume_role" {

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    condition {
      test     = "StringLike"
      variable = "${replace(var.aws_iam_openid_connect_provider_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.env_name}:dbt-duckdb-webinar-*"]
    }

    principals {
      identifiers = [var.aws_iam_openid_connect_provider_arn]
      type        = "Federated"
    }
  }
}

resource "aws_iam_role_policy" "sample_pyspark_3" {
  name   = "dbt-duckdb-webinar"
  role   = aws_iam_role.dbt-duckdb.id
  policy = data.aws_iam_policy_document.dbt_duckdb_webinar.json
}

data "aws_iam_policy_document" "dbt_duckdb_webinar" {
  statement {
    actions = [
      "s3:*"
    ]
    resources = [
      "arn:aws:s3:::conveyor-samples-b9a6edf0",
      "arn:aws:s3:::conveyor-samples-b9a6edf0/*",
    ]
    effect = "Allow"
  }

  statement {
    actions = ["glue:GetDatabase"]
    resources = [
      "*",
      "arn:aws:glue:eu-west-1:130966031144:*",
    ]
  }
  statement {
    actions   = ["glue:CreateDatabase"]
    resources = ["arn:aws:glue:eu-west-1:130966031144:database/default"]
  }

  statement {
    actions = [
      "glue:*"
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:database/coffee_data",
      "arn:aws:glue:${var.aws_region}:${var.aws_account_id}:table/coffee_data/*",
      "arn:aws:glue:eu-west-1:130966031144:catalog",
      "arn:aws:glue:eu-west-1:130966031144:database/coffee_data",
      "arn:aws:glue:eu-west-1:130966031144:table/coffee_data/*"
    ]
    effect = "Allow"
  }
}
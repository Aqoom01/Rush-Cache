# 실행 IAM 사용자 확인. 로컬과 CI 모두 OpenStd 키로만 실행한다.
data "aws_caller_identity" "current" {}

resource "terraform_data" "identity_guard" {
  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.arn == "arn:aws:iam::${var.account_id}:user/${var.iam_user_name}"
      error_message = "IAM 사용자 ${var.iam_user_name}로 실행해야 한다. 현재: ${data.aws_caller_identity.current.arn}"
    }
  }
}

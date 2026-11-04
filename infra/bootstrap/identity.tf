# 같은 계정에 이전 프로젝트 IAM 사용자가 있어 allowed_account_ids만으로는 구분되지 않는다.
data "aws_caller_identity" "current" {}

resource "terraform_data" "identity_guard" {
  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.arn == "arn:aws:iam::${var.account_id}:user/${var.iam_user_name}"
      error_message = "IAM 사용자 ${var.iam_user_name}로 실행해야 한다. 현재: ${data.aws_caller_identity.current.arn}"
    }
  }
}

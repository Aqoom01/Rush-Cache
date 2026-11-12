# 자동 정지로 막을 수 없는 사고(키 유출, destroy 실패)를 알아채기 위한 알림. 차단하지 않는다.
resource "aws_budgets_budget" "monthly" {
  name         = "openkache-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    notification_type          = "ACTUAL"
    threshold                  = var.budget_alert_percent
    threshold_type             = "PERCENTAGE"
    subscriber_email_addresses = [var.budget_email]
  }
}

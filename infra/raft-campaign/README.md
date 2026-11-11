# Detached Raft campaign infrastructure

Creates exactly three m6gd.medium ARM nodes in one existing public subnet, one peer-only security group and two campaign-scoped instance roles. It reuses the existing results bucket and an independent S3 Terraform state key. No NAT, load balancer, public inbound rules, data EBS volume or additional controller instance is created.

Reserve `campaign_id` first, supply `account_id`, `subnet_id`, `ami_id`, `results_bucket` in ignored `terraform.tfvars.json`, and initialize the S3 backend with a unique `raft-campaign/<campaign_id>.tfstate` key. `terraform_aws.py` exports short-lived credentials from the named AWS CLI profile into the Terraform process environment only; it never prints or stores access keys.

Review a saved Terraform plan before applying. The caller must be the expected OpenStd account, and existing nonterminated instances plus these three must not exceed nine. Root gp3 volumes are 20GB each, encrypted, and deleted on instance termination. The experiment's automatic action is stop, not terminate.

Cloud-init installs Java 21, Python 3.11, Matplotlib, SSM and fault tools. Only the uniquely identified instance NVMe device may be initialized; root EBS is excluded by model. Readiness is uploaded to `<campaign_id>/control/bootstrap/node-N-ready.json`. Provisioning does not start the experiment. Prepare the detached job with `raft_ec2_campaign.py prepare --campaign-id <same-id>`, then use its start/status/collect commands.

Terraform owns only resources in this module. Keep the remote state for explicit cleanup after reviewing S3 results. It does not alter the borrowed subnet/VPC or remove existing S3 experiment results.

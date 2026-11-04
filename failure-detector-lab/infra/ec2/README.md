# OpenKache EC2 8-node failure-detector campaign

This Terraform stack creates a disposable eight-node, same-AZ EC2 environment. Node 0 is the
observer, node 1 is the fault target, nodes 2-6 are helpers, and node 7 also coordinates the
campaign. The local Docker campaign is not read, stopped, or modified.

## Safety and evidence boundaries

- `terraform apply` creates billable EC2, public IPv4, EBS, and S3 resources.
- The default campaign runs 11 faults x 5 profiles x 10 seeds = 550 logical runs.
- Results are written under a unique S3 experiment prefix and are never merged with Docker rows.
- Instances stop after a validated campaign by default; EBS and public IPv4 allocation state must
  still be checked. Destroy only after results have been downloaded and verified.
- Standard EC2 instances are isolated VMs but are not proof of eight dedicated physical hosts.
- The I/O scenario stresses a dedicated gp3 data volume. The current detector process does not
  perform cache-server storage I/O, so this is node resource contention evidence, not an end-to-end
  OpenKache SSD-stall claim.

## Before plan

1. Let the local Docker campaign continue unchanged.
2. Verify the JAR exists at `failure-detector-lab/build/libs/` and record its SHA-256.
3. Copy `terraform.tfvars.example` to `terraform.tfvars` and choose a unique experiment ID.
4. Confirm the selected instance type is available in the first AZ returned for the region.
5. Confirm the AWS identity can create EC2, IAM, VPC, S3, SSM-related roles, and security groups.

```bash
cd failure-detector-lab/infra/ec2
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out openkache-fd.tfplan
```

Review the plan and estimated cost before applying it. Do not put credentials, private keys, or
account identifiers in `terraform.tfvars`.

## Automatic flow after apply

1. Cloud-init installs Java 17, AWS CLI, `tc`, `stress-ng`, and `fio`.
2. Every node verifies the exact JAR SHA-256 and uploads a ready marker.
3. Node 7 waits for all eight markers and discovers the tagged private IPs.
4. It creates or resumes a deterministic checkpointed plan.
5. SSM starts one JVM per node, applies faults, collects evidence, and uploads every run to S3.
6. Final validation requires every planned sequence to have a passed checkpoint.
7. A `COMPLETED` marker is uploaded, then all eight instances are stopped when auto-stop is enabled.

Use the output `ssm_session_command` to inspect node 7 without opening SSH. Campaign state is stored
under `s3://<results_bucket>/<experiment_id>/campaign/`.

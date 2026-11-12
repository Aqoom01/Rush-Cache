# OpenKache EC2 5-node failure-detector campaign

This Terraform stack creates a disposable five-node, same-AZ EC2 environment by default. Node 0 is
the observer, node 1 is the fault target, nodes 2-3 are helpers, and node 4 also coordinates the
campaign. `node_count` is parameterized from five to eight. The local Docker campaign is not read,
stopped, or modified.

The default instance is `m6gd.medium`: one non-burstable Graviton2 vCPU, 4 GiB memory, and one
59 GB local NVMe SSD per node. Amazon Linux 2023 ARM64 is selected through the public SSM AMI
parameter. Terraform rejects architecture mismatches, any type with more or fewer than one vCPU,
burstable instances, non-`m6gd` types, and types without at least 59 GB of instance storage.

## Safety and evidence boundaries

- `terraform apply` creates billable EC2, public IPv4, root EBS, and S3 resources. It does not
  create a separate EBS data volume.
- The default campaign runs 11 faults x 5 profiles x 10 seeds = 550 logical runs.
- Results are written under a unique S3 experiment prefix and are never merged with Docker rows.
- Instances stop after a validated campaign by default. Local instance-store data is ephemeral and
  is not retained across stop/terminate, so the S3 `COMPLETED` marker and uploaded run artifacts are
  the durable source of truth. Destroy only after results have been downloaded and verified.
- Standard EC2 instances are isolated VMs but are not proof of dedicated physical hosts.
- The detector state, event log, and GC log share the `m6gd` local NVMe instance store with an
  asynchronous `fio` workload during the I/O scenario. Every I/O run must prove the AWS instance-
  store model, selected mount device, configured queue depth, minimum IOPS, duration, and shared
  filesystem before it can pass. This is controlled local-NVMe contention evidence, not an
  end-to-end OpenKache cache-data workload or EBS performance claim.

## Before plan

1. Let the local Docker campaign continue unchanged.
2. Verify the JAR exists at `failure-detector-lab/build/libs/` and record its SHA-256.
3. Copy `terraform.tfvars.example` to `terraform.tfvars` and choose a unique experiment ID.
4. Confirm the selected instance type is available in the first AZ returned for the region.
   `instance_architecture` must match the selected instance type.
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

When this repository runs in WSL and Linux `aws` is not installed, the guarded wrapper automatically
uses `C:\Program Files\Amazon\AWSCLIV2\aws.exe`. This reuses the Windows CLI profile named by
`AWS_PROFILE`; verify that profile first with `aws sts get-caller-identity` in PowerShell. After the
termination gate passes, the wrapper obtains fresh short-lived credentials with the AWS CLI's
`configure export-credentials` command and passes them only through the Terraform process
environment. It does not print or write those credentials to the repository.

## Wait for five existing instances before creating the lab

Use the guarded wrapper when unrelated EC2 instances are still running. It only observes the exact
instance IDs supplied by the operator. It never stops or terminates them, and it does not inspect the
account-wide instance count. `stopped` is still considered blocking; the gate passes only after every
selected instance is `terminated` or no longer returned by EC2.

The following command waits indefinitely, checks every 60 seconds, validates that exactly five IDs
were supplied, and creates a new plan after the gate passes. It does not create resources:

```bash
BLOCKING_INSTANCE_IDS="i-aaa...,i-bbb...,i-ccc...,i-ddd...,i-eee..." \
BLOCKING_EXPECTED_COUNT=5 \
AWS_PROFILE=openkache \
../../scripts/experiments/ec2/wait-for-existing-instances-and-apply.sh
```

After reviewing this workflow and accepting the billable five-node deployment, add `--apply` for
an unattended transition from waiting to creation. The saved plan is generated after the wait, so it
cannot be a stale pre-wait plan.

```bash
BLOCKING_INSTANCE_IDS="i-aaa...,i-bbb...,i-ccc...,i-ddd...,i-eee..." \
BLOCKING_EXPECTED_COUNT=5 \
AWS_PROFILE=openkache \
../../scripts/experiments/ec2/wait-for-existing-instances-and-apply.sh --apply
```

An exact tag can be used instead of IDs, but IDs are safer. With a tag selector, the script snapshots
the matching active instances once at startup and waits for that fixed set. `Ctrl-C` cancels safely;
Terraform is not invoked until the gate clears. Set `WAIT_TIMEOUT_SECONDS` to use a finite timeout.
For explicit IDs, a first-check `NotFound` is rejected as a possible typo. Use
`--allow-missing-at-start` only when an ID was already terminated long enough to disappear from EC2.

```bash
BLOCKING_TAG_KEY=ExperimentId \
BLOCKING_TAG_VALUE=old-experiment \
BLOCKING_EXPECTED_COUNT=5 \
AWS_PROFILE=openkache \
../../scripts/experiments/ec2/wait-for-existing-instances-and-apply.sh
```

## Run beside existing instances

Use `--allow-coexist` when the existing instances must stay running. This mode never calls stop or
terminate on them. After producing a fresh Terraform plan, it reads every pending/running On-Demand
Standard instance in the region, resolves each type's default vCPUs, adds the planned instances, and
requires the projected total to fit quota `L-1216C47A` while preserving at least one spare vCPU. It
also rejects plans containing deletes, replacements, or in-place updates. Capacity evidence is saved
beside the ignored plan file. Override the safety margin only deliberately with
`COEXIST_MIN_REMAINING_VCPUS`.

```bash
AWS_PROFILE=openkache \
TERRAFORM_BIN=/tmp/terraform-bin/terraform \
../../scripts/experiments/ec2/wait-for-existing-instances-and-apply.sh \
  --allow-coexist
```

Add `--apply` only after reviewing the billable five-node plan. With five existing `m6gd.medium`
instances and five planned `m6gd.medium` instances, the expected regional usage is 10 of 16
Standard On-Demand vCPUs.

## Automatic flow after apply

1. Cloud-init installs Java 17, AWS CLI, `tc`, `stress-ng`, `fio`, and `nvme-cli`.
2. Every node identifies exactly one `Amazon EC2 NVMe Instance Storage` device, mounts it for
   experiment state, verifies the exact JAR SHA-256, and uploads a ready marker with storage evidence.
3. The last node (node 4 by default) waits for every marker and discovers the tagged private IPs.
4. It creates or resumes a deterministic checkpointed plan.
5. SSM starts one JVM per node, applies faults, collects evidence, and uploads every run to S3.
6. Final validation requires every planned sequence to have a passed checkpoint.
7. A `COMPLETED` marker is uploaded, then all instances are stopped when auto-stop is enabled.

Use the output `ssm_session_command` to inspect the coordinator without opening SSH. Campaign state
is stored under `s3://<results_bucket>/<experiment_id>/campaign/`.

## Monitor a running campaign

The campaign runs on the coordinator and continues if the local terminal or Codex is closed. From
the repository root, use the status helper for a one-time snapshot:

```bash
AWS_PROFILE=openkache \
TERRAFORM_BIN=/tmp/terraform-bin/terraform \
failure-detector-lab/scripts/experiments/ec2/status-ec2-campaign.sh
```

Use watch mode for a refresh every 30 seconds. This is read-only; stopping the command with
`Ctrl-C` does not stop the EC2 campaign.

```bash
AWS_PROFILE=openkache \
TERRAFORM_BIN=/tmp/terraform-bin/terraform \
failure-detector-lab/scripts/experiments/ec2/status-ec2-campaign.sh --watch 30
```

The output shows the completion marker, unique passed runs out of 550, historical failed attempts,
the next/current planned sequence, all tagged instance states, and an ETA. The first five passed
runs use the conservative 13-16 hour initial estimate; later output uses the observed average.

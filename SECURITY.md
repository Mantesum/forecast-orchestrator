# Security policy

Please report vulnerabilities privately to `mantesum@projecteol.ru`. Do not open a public
issue before a fix is available when a report concerns arbitrary path deletion, command
execution, credential exposure, publication of unvalidated data, or network-share access.

## Operational boundaries

- Child commands are configured as executable paths and are never invoked through a shell.
- Job YAML is generated from trusted administrator-owned templates.
- Recursive cleanup is constrained to resolved managed roots and requires manifest/READY
  ownership markers.
- The Django VM should mount the publication export read-only.
- NFS must be restricted to a private network or VPN and must not be exposed publicly.
- Future S3 credentials must be injected through environment or systemd credentials, never
  committed to YAML.

Only the latest release receives security fixes until a longer support policy is announced.


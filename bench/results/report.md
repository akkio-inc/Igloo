# Cost comparison: tpch at scale 10

Monthly figures assume the suite runs 24 times a day, each run a separate burst that starts from a stopped warehouse or machine. Always-on Igloo serves them all from one machine.

| Platform | Size | Scale | $/hour | Resume | Cold run | Warm run | $ per warm run | $ per burst | $ per month | Failed |
|---|---|---|---|---|---|---|---|---|---|---|
| igloo, scale to zero | c7g.2xlarge (local disk) | 10 | $0.289 | 15.8s | 14.4s | 11.2s | $0.0009 | $0.0072 | $9.96 | - |
| igloo, scale to zero | c7g.2xlarge (S3) | 10 | $0.289 | 16.1s | 58.9s | 9.4s | $0.0008 | $0.011 | $12.58 | - |
| igloo, scale to zero | c7g.4xlarge (S3, disk cache) | 10 | $0.578 | 15.0s | 7.9s | 5.5s | $0.0009 | $0.013 | $14.38 | - |
| igloo, scale to zero | c7g.4xlarge (local disk) | 10 | $0.578 | 16.1s | 9.2s | 5.5s | $0.0009 | $0.014 | $14.67 | - |
| igloo, scale to zero | c7g.4xlarge (S3, empty disk cache) | 10 | $0.578 | 15.1s | 28.5s | 5.5s | $0.0009 | $0.017 | $16.81 | - |
| igloo, scale to zero | c7g.4xlarge (S3) | 10 | $0.578 | 14.2s | 34.5s | 5.5s | $0.0009 | $0.017 | $17.40 | - |
| igloo, scale to zero | c7g.8xlarge (local disk) | 10 | $1.16 | 17.6s | 7.3s | 4.2s | $0.0014 | $0.027 | $24.58 | - |
| snowflake | XS | 10 | $3.00 | - | 79.1s | 37.2s | $0.031 | $0.116 | $84.56 | - |
| databricks | 2XS | 10 | $2.80 | - | 41.1s | 29.9s | $0.023 | $0.265 | $194 | - |
| igloo | c7g.2xlarge (local disk) | 10 | $0.291 | - | 13.8s | 9.3s | $0.0008 | $0.0011 | $213 | - |
| igloo | c7g.4xlarge (S3) | 10 | $0.580 | - | 31.2s | 5.6s | $0.0009 | $0.0050 | $424 | - |
| igloo | c7g.4xlarge (local disk) | 10 | $0.580 | - | 9.3s | 5.5s | $0.0009 | $0.0015 | $424 | - |

Scale to zero: resume is the median time from a client connecting to a stopped machine until Igloo answered, over every burst measured. A burst is billed for the resume, the cold run and 60s of idleness; the gateway ($3.07/month) and disk ($1.61/month) run all month.

## Warm run time per query (median seconds)

| Query | databricks 2XS sf10 | igloo, scale to zero c7g.2xlarge (local disk) sf10 | igloo, scale to zero c7g.2xlarge (S3) sf10 | igloo c7g.2xlarge (local disk) sf10 | igloo, scale to zero c7g.4xlarge (local disk) sf10 | igloo c7g.4xlarge (S3) sf10 | igloo, scale to zero c7g.4xlarge (S3) sf10 | igloo, scale to zero c7g.4xlarge (S3, empty disk cache) sf10 | igloo, scale to zero c7g.4xlarge (S3, disk cache) sf10 | igloo c7g.4xlarge (local disk) sf10 | igloo, scale to zero c7g.8xlarge (local disk) sf10 | snowflake XS sf10 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q01 | 0.96 | 0.57 | 0.49 | 0.48 | 0.27 | 0.27 | 0.27 | 0.29 | 0.28 | 0.27 | 0.19 | 1.04 |
| q02 | 2.08 | 0.16 | 0.14 | 0.14 | 0.11 | 0.11 | 0.11 | 0.11 | 0.11 | 0.11 | 0.10 | 1.30 |
| q03 | 1.71 | 0.49 | 0.39 | 0.39 | 0.22 | 0.23 | 0.23 | 0.23 | 0.22 | 0.22 | 0.17 | 2.52 |
| q04 | 0.98 | 0.29 | 0.25 | 0.25 | 0.16 | 0.17 | 0.16 | 0.16 | 0.16 | 0.16 | 0.12 | 1.72 |
| q05 | 2.02 | 0.52 | 0.43 | 0.43 | 0.25 | 0.25 | 0.25 | 0.25 | 0.25 | 0.25 | 0.20 | 2.05 |
| q06 | 0.53 | 0.24 | 0.18 | 0.18 | 0.11 | 0.11 | 0.11 | 0.11 | 0.11 | 0.11 | 0.09 | 0.59 |
| q07 | 1.62 | 0.52 | 0.40 | 0.41 | 0.23 | 0.23 | 0.23 | 0.23 | 0.23 | 0.23 | 0.18 | 3.05 |
| q08 | 1.61 | 0.63 | 0.48 | 0.48 | 0.29 | 0.30 | 0.29 | 0.29 | 0.29 | 0.29 | 0.24 | 2.56 |
| q09 | 1.89 | 1.22 | 1.06 | 1.06 | 0.59 | 0.59 | 0.58 | 0.59 | 0.58 | 0.58 | 0.40 | 2.33 |
| q10 | 2.09 | 0.59 | 0.45 | 0.46 | 0.27 | 0.27 | 0.27 | 0.26 | 0.26 | 0.26 | 0.24 | 4.12 |
| q11 | 0.93 | 0.11 | 0.10 | 0.10 | 0.07 | 0.07 | 0.07 | 0.07 | 0.07 | 0.07 | 0.05 | 0.52 |
| q12 | 0.70 | 0.27 | 0.24 | 0.24 | 0.16 | 0.16 | 0.16 | 0.15 | 0.15 | 0.16 | 0.11 | 1.00 |
| q13 | 1.09 | 0.77 | 0.67 | 0.67 | 0.35 | 0.35 | 0.35 | 0.35 | 0.35 | 0.35 | 0.24 | 2.87 |
| q14 | 0.80 | 0.40 | 0.29 | 0.29 | 0.17 | 0.18 | 0.17 | 0.17 | 0.17 | 0.17 | 0.14 | 0.68 |
| q15 | 1.12 | 0.33 | 0.26 | 0.26 | 0.17 | 0.16 | 0.16 | 0.16 | 0.17 | 0.16 | 0.13 | 0.84 |
| q16 | 0.98 | 0.31 | 0.30 | 0.28 | 0.25 | 0.26 | 0.26 | 0.25 | 0.26 | 0.24 | 0.24 | 0.82 |
| q17 | 1.29 | 0.55 | 0.39 | 0.39 | 0.22 | 0.23 | 0.22 | 0.22 | 0.22 | 0.22 | 0.18 | 0.85 |
| q18 | 2.68 | 0.70 | 0.64 | 0.64 | 0.36 | 0.36 | 0.36 | 0.35 | 0.35 | 0.35 | 0.24 | 2.59 |
| q19 | 0.86 | 0.48 | 0.37 | 0.37 | 0.22 | 0.22 | 0.21 | 0.21 | 0.21 | 0.22 | 0.16 | 0.85 |
| q20 | 1.23 | 0.43 | 0.35 | 0.35 | 0.22 | 0.22 | 0.22 | 0.21 | 0.22 | 0.22 | 0.18 | 1.25 |
| q21 | 1.93 | 1.45 | 1.31 | 1.30 | 0.75 | 0.75 | 0.74 | 0.75 | 0.74 | 0.75 | 0.51 | 2.79 |
| q22 | 0.76 | 0.19 | 0.16 | 0.16 | 0.10 | 0.10 | 0.10 | 0.10 | 0.10 | 0.10 | 0.09 | 0.91 |

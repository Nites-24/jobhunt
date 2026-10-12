# Open Source Software Notices & Compliance

This project is licensed under the [MIT License](LICENSE).

## Third-Party Open Source Dependencies

The following open-source packages are used in JobHunt under their respective licenses:

| Package | Purpose | Upstream License |
| :--- | :--- | :--- |
| `boto3` / `botocore` | AWS SDK for Python (DynamoDB, SQS, SNS, S3, Secrets Manager, Bedrock) | Apache License 2.0 |
| `pytest` | Testing framework | MIT License |
| `moto` | AWS service mocking for offline unit and regression tests | Apache License 2.0 |
| `requests` | HTTP client for direct ATS and webhook operations | Apache License 2.0 |
| `pyyaml` | Configuration file parsing | MIT License |

---

## Job Source Terms of Service & Scraping Compliance

When deploying and running JobHunt, users interact with third-party web services and APIs. Contributors and deployers are responsible for adhering to the terms of service of each source provider:

1. **Hacker News ("Who is Hiring")**:
   - Queries the official Algolia HN Search API and HN Firebase endpoints.
   - Intended for personal, non-commercial job-seeking review.
   - Respect polite query intervals and do not perform aggressive scraping.

2. **Direct ATS Job Boards (Greenhouse, Lever, Ashby)**:
   - Queries public company job board endpoints (e.g. `boards-api.greenhouse.io`, `api.lever.co`, `jobs.ashbyhq.com`).
   - Job postings are fetched strictly for individual candidate evaluation.
   - **Do not redistribute, resell, or bulk-republish scraped job descriptions** without express permission from the posting companies.

3. **Google Custom Search JSON API (Deprecation Notice)**:
   - **Important:** Google has closed the Custom Search JSON API to new customers and published a sunset/transition deadline of **January 1, 2027**.
   - JobHunt defaults `GOOGLE_CSE_ENABLED="false"` and operates autonomously without Google CSE by relying on direct ATS board polling and Hacker News.
   - Do not attempt to bypass Google API terms or scrape Google Search result pages directly.

4. **JSearch (RapidAPI)**:
   - Requires user-provided RapidAPI credentials.
   - Bound by RapidAPI terms of use and tier rate limits.

---

## Candidate Privacy & Zero-Trust Notice

- Candidate identity, resumes, personal email credentials, and private feedback are designed to be decoupled from code.
- Deployers should ensure that personal resumes uploaded to S3 or credentials stored in AWS Secrets Manager are protected with least-privilege IAM policies.
- Do not commit candidate resumes or PII to public version control.

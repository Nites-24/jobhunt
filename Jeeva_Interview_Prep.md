# 🎙️ AUDIO PREP: Jeeva.AI Interview Cheat Sheet
*(Read this aloud or use a text-to-speech reader while driving)*

## 1. THE ELEVATOR PITCH (The Hook)
**When they ask:** "Tell me about yourself / Why Jeeva?"

**Your Answer:**
"I’m an AI engineer obsessed with automating outbound workflows. I actually followed what Jeeva is doing with AI sales reps and realized the exact same problem exists for job hunters—it’s manual, repetitive lead-gen and outreach. So, I built a personal, miniature version of Jeeva. I created an autonomous, multi-agent pipeline on AWS that scrapes Hacker News and ATS boards for leads, enriches the data by scoring jobs against my resume using Claude, and automatically drafts personalized outreach emails. I control the entire pipeline through a Telegram bot on my phone. Because I've already built the architecture for AI outbound, coming to Jeeva feels like a natural extension of what I'm already doing."

---

## 2. JD ALIGNMENT: Mapping Your Project to Their Tech Stack

### A. "Multi-agent systems & Agentic workflows"
**How to answer:** 
"My pipeline isn't just one script—it's a multi-agent system. I have an *Ingestor Agent* for data scraping, a *Router Agent* that acts as a control plane via a Telegram webhook, a *Processor Agent* that handles the LLM logic, and a *Sender Agent* for executing actions via the Gmail API. They operate asynchronously to prevent bottlenecks."

### B. "Backend Python, Cloud AWS, Scalable deployment"
**How to answer:** 
"The entire backend is Python running on Serverless AWS Lambdas. To ensure it scales, I decouple the agents using AWS SQS message queues and store state in DynamoDB. If my ingestor pulls 100 leads at once, SQS buffers them so the Bedrock LLM doesn't hit concurrency limits, ensuring zero data loss."

### C. "Integrate external data sources & APIs"
**How to answer:** 
"Data integration is the core of my system. I integrated the Hacker News API for raw lead-gen, Hunter.io for email extraction, Google Custom Search API for ATS scraping, AWS Bedrock for evaluation, and the Telegram API for human-in-the-loop control."

---

## 3. TECHNICAL CHALLENGES YOU SOLVED (To prove you are a Senior)

**Challenge 1: The "Ghost Lead" Data Problem**
*   **The Story:** "Initially, I used a third-party aggregator API for lead gen. But I found that AI is useless if the underlying data is bad—I was getting 'ghost jobs' that were already closed. To fix this, I re-architected my ingestor to perform 'Discoverable Search', bypassing aggregators and directly querying ATS databases (Greenhouse/Lever) via Google Programmable Search, and scraping live Hacker News threads. *Takeaway: High-quality AI requires high-quality data pipelines.*"

**Challenge 2: The Hallucination & Regex Problem**
*   **The Story:** "Cold emails require precision; you can't have an AI hallucinating an email address. I built strict guardrails using regex (`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+`) to extract exact recruiter emails from raw text, and explicitly filtered out `noreply@` or `hr@` aliases so the AI only targets real humans. *Takeaway: I know how to constrain LLMs for production reliability.*"

---

## 4. RAPID FIRE: Potential Questions & Your Answers

**Q: "How do you ensure the AI doesn't send a bad email to a prospect?"**
*A: "Human-in-the-loop architecture. My AI evaluates the lead, drafts the email, and puts it in a 'PENDING_APPROVAL' state in DynamoDB. It pings my Telegram, and the email is only dispatched via Gmail API when I explicitly click 'Approve'. I believe in AI leverage with human oversight."*

**Q: "What LLM frameworks did you use?"**
*A: "I used AWS Bedrock invoking Anthropic's Claude. I avoided overly bloated frameworks when possible, preferring direct API calls with strict system prompts and structured JSON outputs to guarantee deterministic results."*

**Q: "What are your weaknesses or areas to grow?"**
*A: "While I'm highly proficient in tying together cloud infrastructure (AWS) and LLM APIs to build end-to-end products, I'd like to get deeper into the low-level fine-tuning of open-source models (like Llama 3) for highly specific edge cases, rather than relying purely on foundation models."*

---

## 5. QUESTIONS FOR YOU TO ASK THEM
*(Asking good questions proves you understand their business)*
1. "In my pipeline, data quality (avoiding dead jobs) was the hardest part. For Jeeva's Sales Reps, how are you currently solving the 'dirty CRM data' problem before the AI triggers an outreach?"
2. "Are your AI agents primarily fully autonomous, or do you build 'human-in-the-loop' approval gates for your enterprise clients?"
3. "What is the biggest engineering bottleneck preventing Jeeva from scaling 10x right now? Is it LLM latency, data enrichment, or UI/UX?"

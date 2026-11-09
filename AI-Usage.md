# AI Usage

I used Claude and Co-pilot as a development assistant while working on this assignment. I mainly used it to discuss the overall approach, think through edge cases, and get feedback on the implementation.

**Where I used AI:**

- Discussed the worker architecture and how to coordinate four independent processes using PostgreSQL.
- Brainstormed ways to handle message ordering, duplicate deliveries, delayed receipts, and worker failures.
- Used it to review parts of the implementation and identify scenarios I might have missed.

**What I worked on myself:**

I reviewed the suggestions and made the implementation decisions based on the actual assignment requirements. I worked through the database transactions, worker coordination, snapshot and delta processing, and failure-handling logic. I also ran the application, checked the database results, and adjusted the implementation based on what I observed.

I didn't blindly copy AI-generated code. I reviewed and modified suggestions to make sure they fit the existing schema and the requirements.

**One example where AI got something wrong:**

One of the initial suggestions was to treat a permanently failed receipt as a message that could simply be skipped so processing could continue. On reviewing the sequence requirements, I realized that this could leave a gap in the store's update history and make the final stock inaccurate.

I changed the approach so that a permanently failed receipt blocks further processing for that particular store, while other stores can continue processing independently.

**Overall:**

I used AI to speed up research, brainstorming, and code review, but I took responsibility for understanding the design, making the implementation decisions, and verifying the behavior through testing.
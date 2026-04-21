# DataOps & dbt Mentorship Program

**Duration:** 6 Weeks (2 Hours per Week)
**Tech Stack:**, PostgreSQL, dbt, Airflow
**Team:** Marwan, Abdul Ahad, Ghazi Alchamat

---

## Week 1: Sources, Models, and Seeds

- **Sources:** How to tell dbt where to find the raw data already loaded into our database.
- **The Two Layers:** We will write `STAGE` models to clean the raw data (fixing names and formats). Then, we will write `DEV` models to do the math (like calculating total orders).
- **Seeds:** How to take a simple CSV file (like a list of store locations) and upload it directly into PostgreSQL so we can join it with our data.

## Week 2: Materializations

- **Tables vs. Views:** Understanding the best way to save our data in PostgreSQL so dashboards load fast.
- **Incremental Loads:** Building tables that only add _new_ records, saving a lot of time and computer power.
- **Snapshots:** How to take an automatic "picture" of data so we can look back and see how things changed over time.

## Week 3: Data Tests

- **Basic Checks:** Making sure important columns are never empty and never have duplicate IDs.
- **Custom Business Rules:** Writing SQL checks for specific rules (for example, making sure an order date is never in the future).
- **Saving Bad Data:** How to take failing rows and save them in a special "quarantine" table so we can investigate them later.

## Week 4: Macros and Packages

- **Jinja Basics:** Using variables and loops inside our SQL files to automate repetitive tasks.
- **Creating Macros:** Building custom functions (like a standard currency converter) that everyone on the team can share.
- **Community Packages:** Downloading free, pre-written code from the dbt community so we don't have to reinvent the wheel.

## Week 5: Hooks, Exposures, and Documentation

- **Database Hooks:** Running special commands right after a table finishes building (like creating an index in PostgreSQL to make queries faster).
- **Exposures:** Keeping track of which dashboards rely on which dbt models, so we know who to warn if a table breaks.
- **Writing Descriptions:** Adding plain-English notes to our tables and columns so anyone can understand them.

## Week 6: Airflow Automation

- **Airflow Basics:** Learning how Airflow tells different tools when it is their turn to run.
- **The Pipeline:** We will build a complete workflow that orchestrates dbt to test, and build the final tables on a schedule.

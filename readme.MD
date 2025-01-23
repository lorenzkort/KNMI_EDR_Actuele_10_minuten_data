## Dataset
KNMI collects observations from the automatic weather stations situated in the Netherlands and BES islands on locations such as aerodromes and North Sea platforms. In addition, wind data from KNMI wind poles are included. The weather stations report every 10 minutes meteorological parameters such as temperature, relative humidity, wind, air pressure, visibility, precipitation, and cloud cover. The number of parameters differs per station. The file for the past 10 minutes is available a few minutes later and contains a timestamp denoting the end of the observation period in UTC. It is possible that a station's observations may not be immediately available. Files are updated with missing data up to 4 hours later.

## Expected Deployment Environment
AWS-managed Airflow

### Building and running your application

When you're ready, start your application by running:
`docker compose up --build`.

Your application will be available at http://localhost:8000.

### Deploying your application to the cloud

First, build your image, e.g.: `docker build -t myapp .`.
If your cloud uses a different CPU architecture than your development
machine (e.g., you are on a Mac M1 and your cloud provider is amd64),
you'll want to build the image for that platform, e.g.:
`docker build --platform=linux/amd64 -t myapp .`.

Then, push it to your registry, e.g. `docker push myregistry.com/myapp`.

Consult Docker's [getting started](https://docs.docker.com/go/get-started-sharing/)
docs for more detail on building and pushing.

### References
* [Docker's Python guide](https://docs.docker.com/language/python/)


DONE:
* Write a robust pipeline to extract and transform data in Python from the following API
https://developer.dataplatform.knmi.nl/edr-api which contains the data set “Actuele 10
minuten data” and stores the data into a database system of your choice.
* Dockerize your code
* Create a stack (e.g. using docker compose) that includes your own container for the data
pipeline and the containers of the tools of your choice (database, orchestrator, etc). The
stack should include multiple containers. You have the freedom to choose what you think
is important.


TODO before tonight
* A visual diagram or high-level description of the architecture of a (modular) pipeline (e.g.
including a scraper; load step; ingest) including how it will be read, orchestrated and
stored.
* Provide instructions on how we can run your system on our laptops.

* Testing of data in TimeScaleDB
* Add orchestrator?
* Robustness

Writing:
* Describe the reasoning of the steps you take and share your reasoning for making these
choices.
* Write up your thoughts and ideas on how you could bring this pipeline into a production
system.
* What data transformations (if any) would you recommend to make this a
convenient dataset to be used by a data scientist?
* What is working well already?
    * API
* What is still missing?
    * Periodic full-load and versioning of the table
    * Logging to a centralised logging solution such as Grafana, Datadog
    * CI/CD Deployment process to Production
        * Connection to Dev Snowflake server
        * Technical validation & Data Quality testing
    * Airflow task to periodically check the Swagger documentation to see if anything changed for calling the API

* Is it scalable?
* What makes it robust?
* Other thoughts?
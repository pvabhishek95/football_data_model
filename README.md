# Environment setup for the application

## Pre-requisite

To run the code, you will need

1. [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)(v1.27.0 or later)
2. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Clone the git repo and run the ETL as shown below.

```bash
git clone https://github.com/pvabhishek95/football_data_model.git
cd football_data_model
make up
make run-etl # run the ETL process
make down # spins down the containers
```

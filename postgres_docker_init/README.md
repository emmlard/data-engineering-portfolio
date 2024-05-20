## Setup Basic Postgres Infrastructure with Docker

### Overview
The main aim of this project is to set up basic PostgreSQL infrastructure using Docker and Docker Compose, load data, and interact with PostgreSQL from Python.

### Structure
```plaintext
postgres_docker_init/
├── data/
│   └── your_data.csv
├── infra_script/
│   └── init.sql
├── src/
│   └── main.py
├── docker-compose.yml
└── README.md
```

### How It Works

1. **Docker Setup**:
    - A `docker-compose.yml` file is used to define and run a PostgreSQL container.
    - The container maps port 5434 on your local machine to port 5432 in the Docker container to avoid conflicts with any local PostgreSQL installations.

2. **Database Initialization**:
    - An SQL script located in the `infra_script` folder initializes the database by creating a new schema and table, then loads data from a CSV file placed in the `data` folder.

3. **Python Interaction**:
    - A Python script in the `src` folder connects to the PostgreSQL database and executes a query to count the number of records in the table.

## How to Run

### Prerequisites

- Docker: [Install Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Install Docker Compose](https://docs.docker.com/compose/install/)
- Python: [Install Python](https://www.python.org/downloads/)
- Install psycopg2 and python-dotenv packages:
    ```sh
    pip install psycopg2 python-dotenv
    ```

### Steps

1. **Clone the Repository**:
    ```sh
    git clone https://github.com/emmlard/data-engineering-portfolio-projects.git
    cd data-engineering-portfolio-projects/postgres_docker_init
    ```

2. **Place Your CSV File**:
    - Place your CSV file in the `data` folder. Ensure the filename matches what is referenced in your `init.sql` script.

3. **Update Environment Variables**:
    - Create a `.env` file in the root of `postgres_docker_init` with the following content and provide the credentials:
      ```env
      POSTGRES_USER=your_username
      POSTGRES_PASSWORD=your_password
      POSTGRES_DB=your_database
      POSTGRES_HOST=localhost
      POSTGRES_PORT=5434
      ```

4. **Spin Up the Server**:
    - Start the PostgreSQL container using Docker Compose:
      ```sh
      docker-compose up
      ```
    #### Note: Managing State File
    When you start Docker Compose, a folder `pg_data` will be created in your project root folder. If you ever want to clear state information, simply stop your Docker Compose service using:
      ```sh
      docker-compose down
      ```
    and delete the `pg_data` folder. This will allow Docker to recreate PostgreSQL from scratch and reload your data from the `./data` folder.

5. **Run the Python Script**:
    - In a new terminal, navigate to the `src` directory and run the Python script:
      ```sh
      cd src
      python main.py
      ```

### Final Results

After running the Python script, you should see the count of records in your table printed to the console. This demonstrates the successful setup, data loading, and interaction with PostgreSQL.

### Technology Used

- **Docker**: For containerizing the PostgreSQL database.
- **Docker Compose**: For managing multi-container Docker applications.
- **Python**: For scripting and database interaction.
- **psycopg2**: Python PostgreSQL adapter for database connection and queries.
- **dotenv**: For managing environment variables securely.

### Conclusion

This project demonstrates a basic setup of PostgreSQL infrastructure using Docker and Docker Compose and interacting with the database using Python. It serves as a foundation for more complex data engineering tasks and showcases essential skills in managing data infrastructure.
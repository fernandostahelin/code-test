# overview
I opted to run the scheduling of the procedures inside the own procedure, as a matter of simplicity.

I could've utilized Airflow to do so, but I opted for this simpler approach.

Docker was used to containerize the database instances. You can replicate the environment by running `make up`. or `docker compose up -d`.
Keep in mind that you'll need to define the environment variables in a `.env` file.

# Task 01
## Running the procedures
Run the .sh files inside each task folder.
The first run will make the procedure appear in the postgres.

I called the procedure using PGAdmin.
`call northwind.public.process_data(5);`
The argument defines in which step the procedure will fail.

You can query the tables below to see the results after the procedure has run, or while it is running.
Also, I am printing some of the logs to the console.

### View logs
Using dbeaver

````
SELECT routine_name, routine_type 
FROM information_schema.routines 
WHERE routine_schema = 'public';
````
`````
select * from northwind.public.long_process_data;

select * from northwind.public.process_execution_log;
`````
# Task 02
Run the task_02.sh file in sql/task_02.

It will create the tables in the target and run the python script using uv, the package manager I choosed for this project.

Keep in mind the .sh file is idempotent. It drops the existing tables in the target, creates new ones and runs the python script.

If you want to check the incremental behavior, just remove the `DROP TABLE IF EXISTS` lines in the .sql files.

# Task 03
I created the schema as code using https://dbdiagram.io/home/ and then export as a .sql DDL file.

# Task 04
The same as the before, run the .sh file that does everything.

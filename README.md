# overview
I opted to run the scheduling of the procedures inside the own procedure, as a matter of simplicity.

I could've utilized Airflow to do so, but I opted for this simpler approach.

Docker was used to containerize the database instances. You can replicate the environment by running `make up`. or `docker compose up -d`.
Keep in mind that you'll need to define the environment variables in a `.env` file.

# Running the procedures
Run the .sh files inside each task folder.
The first run will make the procedure appear in the postgres.

I did run the procedure in the DBeaver UI using `call process_data();`

## View logs
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



This notebook initializes and creates a table from a CSV file containing all of the product information about Alko's products. It then modifies/casts some column names to improve readability, such as removing percentage signs. Afterward, it creates Delta tables that contain all of the red wines and white wines. These tables can be easily queried, and you can perform operations like joins on them.

One important note is that I ran everything on a Docker image I created. This image is essentially a Jupyter notebook that contains all the dependencies you need for this task. The main reason for this choice is that running Spark locally, especially on Windows, is much more difficult than running it in a containerized environment.


I also created a simple airflow script that just reads the csv and transforms it to a delta table. If you try to do the same and are using docker remember chmod. Airflow does not have read or write rights to new files you create.

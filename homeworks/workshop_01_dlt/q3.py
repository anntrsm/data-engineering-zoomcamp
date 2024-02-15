import dlt
import duckdb


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_1():
    print(person)

for person in people_2():
    print(person)

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')
info = generators_pipeline.run(people_1(), table_name="people", write_disposition="replace")
info = generators_pipeline.run(people_2(), table_name="people", write_disposition="append")
print("\n", info)

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('\nLoaded tables: ')
print(conn.sql("show tables"))

print("\n people table below:")
people = conn.sql("SELECT SUM(age) FROM people").df()
print(people)
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

table = []
for sqrt_value in generator:
    print(sqrt_value)
    table.append(sqrt_value)

# Question 1. What is the sum of the outputs of the generator for limit = 5?
print( sum(table) )

# Question 2. What is the 13th number yielded by the generator?
limit2 = 13
generator2 = square_root_generator(limit2)
list_of_values_gen_2 = [value for value in generator2] # Grab the last value
last_value = list_of_values_gen_2[-1]
print(last_value)
# Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
"""
Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data

1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
2. Append the second generator to the same table as the first.
3. After correctly appending the data, calculate the sum of all ages of people.

"""
import dlt


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

ages_people = []
data = []
for person in people_1():
    print(person)
    ages_people.append( person.get("Age") ) # get the value of the key "Age"
    data.append(person)

print(f'The age is {sum(ages_people)}')

# define the connection to load to. 
# We now use duckdb, but you can switch to Bigquery later
pipeline = dlt.pipeline(pipeline_name="test",
						destination='duckdb', 
						dataset_name='test_users')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(data, 
                    table_name="users", 
                    write_disposition="replace")

# show the outcome
print(info)

# Second Generator
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

data2 = []
for person in people_2():
    print(person)
    ages_people.append( person.get("Age") )
    data2.append(person)

print(f'The age is {sum(ages_people)}')


#pipeline = dlt.pipeline(destination='duckdb', dataset_name='test')


# run the pipeline with default settings, and capture the outcome
info = pipeline.run(data2, 
					table_name="users", 
					write_disposition="merge",
                     primary_key="ID",
                      )

# show the outcome
print(info)

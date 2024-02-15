def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1


limit = 5
s = 0
generator = square_root_generator(limit)

for sqrt_value in generator:
    s = s + sqrt_value
    print(sqrt_value)
print('summary of 5', s)
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1


limit = 13
generator = square_root_generator(limit)

for sqrt_value in generator:
    print(sqrt_value)
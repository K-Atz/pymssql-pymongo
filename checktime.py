import time

start = time.time()
s = time.process_time()
print("hello")
end = time.time()
e = time.process_time()
print(end - start, e - s)
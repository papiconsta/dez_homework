import sys
print("arguments:", sys.argv)

day = int(sys.argv[1])
month = int(sys.argv[2])
year = int(sys.argv[3])

print(f"Processing date: {day:02d}-{month:02d}-{year}")
import subprocess
import time

# execute 
start_time = time.time()
result = subprocess.run(
    ['python3', '/Web Extraction/to_pgres.py'],
    capture_output=True, text=True)
end_time = time.time()

print(f"Execution time: %s seconds" % (end_time - start_time))
print(result.stderr)

#
# snippet just to print out the contents of the log file created in the first snippet.
#

import json
import time
target_file = '/home/naya/tutorial/kafka/srcFiles/srcFile.log'

# Print the data on file
with open(target_file) as f:
    print(f.read())
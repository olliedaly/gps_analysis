import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import os

root_path = "Geolife Trajectories 1.3/Data"
header_list = ['lat', 'long', '0', 'altitude', 'days since 18891230', 'date', 'time']
header_string = ",".join(header_list) + "\n"

for root, dirs, files in os.walk(root_path):
    for filename in files:
        filename = os.path.join(root, filename)
        # Skip non plt files
        if not filename.endswith(".plt"):
            continue

        try:
            with open(filename, "r") as f:
                lines = f.readlines()

                # Remove the first 6 lines
                lines_after_removal = "".join(lines[6:])

                # Consider checking first line, or sample of lines, if performance is an issue
                for line in lines:
                    fields = line.strip().split(",")
                    if len(fields) != 7:
                        raise ValueError(
                            "Incorrect number of fields in {filename}."
                        )

                # Add column header
                final_content = header_string + "".join(lines_after_removal)

        except FileNotFoundError:
            print(f"Error: The file '{filename}' was not found.")
        except Exception as e:
            print(f"An error occurred while opening or reading the file: {e}")

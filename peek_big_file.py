import os


with open("asapbio.json", "r") as file:

    part_number = 0

    while True:

        part_content = file.read(1000000000)

        if not part_content:
            break

        part_number += 1

        with open(f"asapbio.json.part.${part_number}", "w") as out:
            out.write(part_content)


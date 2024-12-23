import json
import os
from time import sleep
from faker import Faker

faker = Faker()

stream_dir_dump = "data/stream"
batch_size = 1000


def cleanup():
  for file in os.listdir(f"{stream_dir_dump}"):
    os.remove(f"{stream_dir_dump}/{file}")


def generate_json():
  return {
    "id": faker.uuid4(),
    "name": faker.name(),
    "address": {
      "street": faker.street_address(),
      "city": faker.city(),
      "state": faker.state(),
      "zip": faker.zipcode(),
    },
  }


cleanup()

for i in range(100):
  with open(f"{stream_dir_dump}/data_{i}.json", "w") as f:
    for j in range(batch_size):
      json.dump(generate_json(), f)
      f.write("\n")
  sleep(0.1)

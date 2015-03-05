import json
import sys

def main(fn):
    with open(fn) as f:
        my_list = [item['private'].partition('.')[0]  for item in json.loads(f.read())]
    print ' '.join(my_list)

if __name__ == "__main__":
    main(sys.argv[1])

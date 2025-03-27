import argparse
import json
from core.database import Database

def main():
    parser = argparse.ArgumentParser(description="PSDB CLI")
    parser.add_argument("command", choices=["insert", "find", "update", "delete"], help="CRUD commands")
    parser.add_argument("--key", required=True, help="Key for operation")
    parser.add_argument("--value", help="Value for insert/update (in JSON format)")

    args = parser.parse_args()
    db = Database()

    if args.command in ["insert", "update"] and args.value:
        try:
            value = json.loads(args.value.replace("'", "\""))
        except json.JSONDecodeError:
            print("Invalid JSON format. Use double quotes or proper JSON formatting.")
            return
    else:
        value = None

    if args.command == "insert":
        db.insert(args.key, value)
    elif args.command == "find":
        print(db.find(args.key))
    elif args.command == "update":
        db.update(args.key, value)
    elif args.command == "delete":
        db.delete(args.key)

if __name__ == "__main__":
    main()

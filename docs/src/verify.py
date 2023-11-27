import os
import json

def read_yes_no(text) -> bool:
    print(text)
    while True:
        choice = input().lower()
        if choice in ["y", "yes"]:
            return True
        elif choice in ["n", "no"]:
            return False
        else:
            print("Please respond with 'y/es' or 'n/o'")


def try_parse_frontmatter(f, filename):
    frontmatter = ""
    line = f.readline()
    if not line.startswith("---"):
        raise ValueError(f"Invalid frontmatter in {filename}: could not find starting '---'")
    
    line = f.readline()
    while not line.startswith("---"):
        frontmatter += line
        line = f.readline()

    # Parse the frontmatter as JSON
    try:
        return json.loads(frontmatter)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid frontmatter in {filename}: {e}")


def main():

    # Recursively find all doc items in the functions directory
    items = { }
    aliases = { }
    for root, _, files in os.walk("functions"):
        for file in files:
            if file.endswith(".md"):
                # Get the file base name (without path and extension)
                base = os.path.splitext(os.path.basename(file))[0]

                # Parse the frontmatter
                with open(os.path.join(root, file), 'r') as f:
                    try:
                        info = try_parse_frontmatter(f, base)
                        items[base] = info
                        if 'aliases' in info:
                            for alias in info['aliases']:
                                if alias in aliases:
                                    print(f"Duplicate alias '{alias}' in {base}")
                                aliases[alias] = info['id']
                    except ValueError as e:
                        pass # Ignore invalid frontmatter                    


    # Read the scalar_functions.json file
    # TODO: Call DuckDB and generate these instead
    collections = [
        {'file': 'functions/scalar_functions.json', 'doc_type': 'scalar_function'}, 
        {'file': 'functions/aggregate_functions.json', 'doc_type': 'aggregate_function'},
        {'file': 'functions/table_functions.json', 'doc_type': 'table_function'},
    ]
    for collection in collections:
        with open(os.path.join(os.path.dirname(__file__), collection['file'])) as f:
            doc_type = collection['doc_type']

            for line in f:
                func = json.loads(line)
                id = func['id']

                if id not in items:
                    if id in aliases:
                        print(f"Found alias for scalar function {id} => {aliases[id]}, skipping...")
                        continue

                    print(f"Missing doc item for scalar function {id}")
                    if read_yes_no(f"Create doc item for scalar function {id}? (y/n)"):
                        print(f"Signatures: {func['signatures']}")
                        print("Summary:")
                        summary = input()
                        sigs = []
                        for sig in func['signatures']:
                            print(f"Provide parameter names for signature: {sig}")
                            params = []
                            for i in range(len(sig['parameters'])):
                                param_type = sig['parameters'][i]
                                print(f"Parameter {i} ({param_type}):")
                                params.append({
                                    "name": input(),
                                    "type": param_type,
                                })
                            sigs.append({
                                "returns": sig['returns'],
                                "parameters": params,
                            })

                        print(f"Tags: (space separated)")
                        tags = input().split(" ")

                        items[id] = {
                            "type": doc_type,
                            "title": func['title'],
                            "id": func['id'],
                            "signatures": sigs,
                            "summary": summary,
                        }

                        if len(tags) > 0:
                            items[id]['tags'] = tags

                        with open(os.path.join("functions", id + ".md"), 'w') as f:
                            f.write("---\n")
                            f.write(json.dumps(items[id], indent=4))
                            f.write("\n---\n")
                            f.write("\n### Description\n\n")
                            f.write("TODO\n\n")
                            f.write("### Examples\n\n")
                            f.write("TODO\n\n")

                    continue


                #print(f"Found doc item for scalar function {id}")
                # Check if signatures match
                for func_sig in func['signatures']:
                    match = False
                    for doc_sig in items[id]['signatures']:

                        if doc_type != "table_function" and doc_sig['returns'] != func_sig['returns']:
                            continue

                        if len(doc_sig['parameters']) != len(func_sig['parameters']):
                            continue

                        parameter_mismatch = False
                        for i in range(len(func_sig['parameters'])):
                            if doc_sig['parameters'][i]['type'] != func_sig['parameters'][i]:
                                parameter_mismatch = True
                                break
                            
                        if parameter_mismatch:
                            continue
                        
                        match = True
                        break

                    if match:
                        #print(f"Found\tsignature for scalar function {id}:\t{func_sig}")
                        pass
                    else:
                        print(f"Missing\tsignature for scalar function {id}:\t{func_sig}")                    
                        

if __name__ == "__main__":
    main()
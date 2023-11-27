#!/usr/bin/env python3
import os
import json


def write_function(f, info, content):
    f.write(f"## {info['title']}\n")
    f.write(f"\n_{info['summary']}_\n\n")
    # Write signatures
    for signature in info['signatures']:
        params = ', '.join([f"{param['name']} __{param['type']}__" for param in signature['parameters']])
        if 'returns' not in signature:
            sig = f"{info['title']}({params})"
        else:
            sig = f"__{signature['returns']}__ {info['title']}({params})"
        f.write(f"- {sig}\n")
    
    f.write("\n")

    # Write list of aliases
    if 'aliases' in info and info['aliases']:
        f.write(f"\n__aliases:__ {', '.join(info['aliases'])}\n")

    # Finally write the content
    f.write(f"{content}\n")

    # Write some whitespace too
    f.write("\n")


def main():
    tagged_items = { }
    scalar_functions = { }
    aggregate_functions = { }
    table_functions = { }
    items = []

    # Recursively find all doc items in the functions directory
    for root, _, files in os.walk("functions"):
        for file in files:
            if file.endswith(".md"):
                items.append(os.path.join(root, file))

    # Sort the items by name
    items.sort()

    # Read all the items and parse the frontmatter
    for item in items:
        with open(item, 'r') as f:
            frontmatter = ""
            line = f.readline()
            if not line.startswith("---"):
                raise ValueError(f"Invalid frontmatter in {item}: could not find starting '---'")
            
            line = f.readline()
            while not line.startswith("---"):
                frontmatter += line
                line = f.readline()

            # Parse the frontmatter as JSON
            try: 
                data = json.loads(frontmatter)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid frontmatter in {item}: {e}")
            
            # Add the item to the appropriate list
            doc_type = data['type']
            doc_id = data['id']
            doc_item = {
                'meta': data,
                'content': f.read().strip()
            }
            if doc_type == 'scalar_function':
                scalar_functions[doc_id] = doc_item
            elif doc_type == 'aggregate_function':
                aggregate_functions[doc_id] = doc_item
            elif doc_type == 'table_function':
                table_functions[doc_id] = doc_item
            else:
                raise ValueError(f"Invalid doc type {doc_type} in {item}")
            
            # Add the item id to the tagged items list
            if 'tags' in data and data['tags']:
                for tag in data['tags']:
                    if tag not in tagged_items:
                        tagged_items[tag] = []
                    tagged_items[tag].append(doc_id)

  
    # Now write the combined output file one step up
    outfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "docs.md")
    with open(outfile, 'w') as f:
        
        # Write the header
        f.write("# DuckDB Spatial Extension\n\n")

        # Write the intro from the into.md file
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "intro.md"), 'r') as intro:
            f.write(intro.read())

        f.write("\n")

        # Write all scalar functions
        if scalar_functions:
            f.write(f"# Scalar Functions\n\n")

            # Write ToC
            f.write(f"| Name | Summary |\n")
            f.write(f"| ---- | ----------- |\n")
            for func in scalar_functions.values():
                f.write(f"| [{func['meta']['title']}](##{func['meta']['id']}) | {func['meta']['summary']} |\n")
            f.write("\n")

            # Write individual functions
            for func in scalar_functions.values():
                write_function(f, func['meta'], func['content'])

        # Write all aggregate functions
        if aggregate_functions:
            f.write(f"# Aggregate Functions\n\n")

            # Write ToC
            f.write(f"| Name | Summary |\n")
            f.write(f"| ---- | ----------- |\n")
            for func in aggregate_functions.values():
                f.write(f"| [{func['meta']['title']}](##{func['meta']['id']}) | {func['meta']['summary']} |\n")
            f.write("\n")

            # Write individual functions
            for func in aggregate_functions.values():
                write_function(f, func['meta'], func['content'])

        # Write all table functions
        if table_functions:
            f.write(f"# Table Functions\n\n")
            for func in table_functions.values():
                write_function(f, func['meta'], func['content'])

        if tagged_items:
            # Write all tagged items
            f.write(f"# Functions by tag\n\n")
            for tag, entries in tagged_items.items():
                f.write(f"__{tag}__\n")
                for entry in entries:
                    if entry in scalar_functions:
                        f.write(f"- [{scalar_functions[entry]['meta']['title']}](##{entry})\n")
                    elif entry in aggregate_functions:
                        f.write(f"- [{aggregate_functions[entry]['meta']['title']}](##{entry})\n")
                f.write("\n")


    
if __name__ == '__main__':
    main()
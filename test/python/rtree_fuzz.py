import duckdb
from random import randrange

MAX_WIDTH = 1000
MAX_HEIGHT = 1000

def setup(con):
    con.execute("CREATE TABLE tbl (geom GEOMETRY)")

def generate_rectangle():
    # Generate a random rectangle, making sure that the
    # x1 < x2 and y1 < y2
    x1 = randrange(MAX_WIDTH - 1)
    x2 = randrange(x1, MAX_WIDTH)
    y1 = randrange(MAX_HEIGHT - 1)
    y2 = randrange(y1, MAX_HEIGHT)
    return (x1, y1, x2, y2)

def generate_insert(con):
    # Generate a random rectangle
    rect = generate_rectangle()
    # Generate a random number of points
    num_points = randrange(1000)
    #print("Generating insert: {0} points in rectangle {1}".format(num_points, rect))
    stmt = "INSERT INTO tbl SELECT geom FROM ST_GeneratePoints({{min_x: {0}, min_y: {1}, max_x: {2}, max_y: {3}}}::BOX_2D, {4}) as pts(geom)".format(rect[0], rect[1], rect[2], rect[3], num_points)
    con.execute(stmt)
    res = con.fetchall()
    print("Inserted {0} rows in rectangle: {1}".format(res[0][0], rect))

def generate_delete(con):
    # Generate a random rectangle
    rect = generate_rectangle()
    stmt = "DELETE FROM tbl WHERE ST_Within(geom, ST_MakeEnvelope({0}, {1}, {2}, {3}))".format(rect[0], rect[1], rect[2], rect[3])
    con.execute(stmt)
    res = con.fetchall()
    print("Deleted {0} rows in rectangle: {1}".format(res[0][0], rect))

def generate_query(con):
    # 50% chance of deleting
    if randrange(2) == 0:
        return generate_delete(con)
    else:
        return generate_insert(con)



import os
if os.path.exists("fuzz.db"):
    os.remove("fuzz.db")

con = duckdb.connect("fuzz.db", config = {"allow_unsigned_extensions": "true"})
# load the rtree extension


EXTENSION_PATH = "build/release/extension/spatial/spatial.duckdb_extension"

con.execute(f"LOAD '{EXTENSION_PATH}';")

setup(con)

con.execute("CREATE INDEX idx ON tbl USING rtree(geom)")

checkpoints = 0
restarts = 0
hard_restarts = 0

for i in range(10000):
    if(i % 500 == 0):
        if randrange(4) == 0:
            # Checkpoint
            print("\n\n--- CHECKPOINTING ---\n\n")
            con.execute("CHECKPOINT")
            checkpoints += 1
        else:
            if randrange(4) == 0:
                print("\n\n--- RELOADING EXTENSION ---\n\n")
                # Disconnect
                con.close()
                con = duckdb.connect("fuzz.db", config = {"allow_unsigned_extensions": "true"})
                con.execute(f"LOAD '{EXTENSION_PATH}';")
                restarts += 1
            else:
                con.execute("pragma disable_checkpoint_on_shutdown")
                print("\n\n--- HARD RESTARTING ---\n\n")
                # Disconnect
                con.close()
                print("Reconnecting...")
                con = duckdb.connect(config = {"allow_unsigned_extensions": "true"})
                print("Reconnected")
                con.execute(f"LOAD '{EXTENSION_PATH}';")
                print("loaded extension")
                con.execute("ATTACH 'fuzz.db'")
                print("attached")
                con.execute("USE fuzz")
                hard_restarts += 1
                print("\n\n--- HARD RESTARTED ---\n\n")

    else:
        generate_query(con)


print("Checkpoints: {0}".format(checkpoints))
print("Restarts: {0}".format(restarts))
print("Hard restarts: {0}".format(hard_restarts))

with open("metadata_inserts.cql", "w") as f:
    f.write("BEGIN BATCH\n")
    for i in range(50):
        slot = i + 1
        sensor_id = f"S{slot}"
        x = (i % 10) * 100
        y = (i // 10) * 100
        f.write(
            f"INSERT INTO parking_keyspace.metadata "
            f"(sensor_id, slot_id, x, y) VALUES "
            f"('{sensor_id}', '{sensor_id}', {x}, {y});\n"
        )
    f.write("APPLY BATCH;\n")
print("Wrote metadata_inserts.cql")
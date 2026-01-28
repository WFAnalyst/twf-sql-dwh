# TWF SQL Data Warehouse
Files related to the development of a data warehouse to enable and support data continuous integration.

This project is a collection of scripts, designs, changes and architecture plans associated with creating a data infrastructure for the foundation.

![img](./docs/Data%20Architecture.png)

## Phase 1: Extracting and Loading relevant data into bronze

The first phase will focus on extracting data from various systems and loading it into the bronze layer of the warehouse.

|Source| Load Strategy | Object Type(s) | Destination |
|------|---------------|----------------|-------------|
| FMS  | Incremental   | SQL Tables     | SQL DB      |
| GMS  | Incremental   | JSOn Objects| SQL DB |
| Ops  | Incremental   | Flat Files | SQL DB |

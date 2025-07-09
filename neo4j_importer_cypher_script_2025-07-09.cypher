:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'Authors.csv',
  file_1: 'Datasets.csv',
  file_2: 'Models.csv',
  file_3: 'ModelswithData.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 2025.6.0.
CREATE CONSTRAINT `username:ID(Author)_Authors_uniq` IF NOT EXISTS
FOR (n: `Authors`)
REQUIRE (n.`username:ID(Author)`) IS UNIQUE;
CREATE CONSTRAINT `model_id_Models_uniq` IF NOT EXISTS
FOR (n: `Models`)
REQUIRE (n.`model_id`) IS UNIQUE;
CREATE CONSTRAINT `dataset_id_Datasets_uniq` IF NOT EXISTS
FOR (n: `Datasets`)
REQUIRE (n.`dataset_id`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`username:ID(Author)` IN $idsToSkip AND NOT row.`username:ID(Author)` IS NULL
CALL {
  WITH row
  MERGE (n: `Authors` { `username:ID(Author)`: row.`username:ID(Author)` })
  SET n.`username:ID(Author)` = row.`username:ID(Author)`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`model_id` IN $idsToSkip AND NOT row.`model_id` IS NULL
CALL {
  WITH row
  MERGE (n: `Models` { `model_id`: row.`model_id` })
  SET n.`model_id` = row.`model_id`
  SET n.`library_name` = row.`library_name`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`dataset_id` IN $idsToSkip AND NOT row.`dataset_id` IS NULL
CALL {
  WITH row
  MERGE (n: `Datasets` { `dataset_id`: row.`dataset_id` })
  SET n.`dataset_id` = row.`dataset_id`
  SET n.`downloads` = toInteger(trim(row.`downloads`))
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Authors` { `username:ID(Author)`: row.`Author_ID` })
  MATCH (target: `Models` { `model_id`: row.`model_id` })
  MERGE (source)-[r: `AUTHOR_WITH_MODEL`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Authors` { `username:ID(Author)`: row.`authorID` })
  MATCH (target: `Datasets` { `dataset_id`: row.`dataset_id` })
  MERGE (source)-[r: `AUTHOR_WITH_DATASET`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_3) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Models` { `model_id`: row.`Model_id` })
  MATCH (target: `Datasets` { `dataset_id`: row.`dataset_id` })
  MERGE (source)-[r: `MODEL_WITH_DATASET`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

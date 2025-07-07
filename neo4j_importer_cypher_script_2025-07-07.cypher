:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'Authors.csv',
  file_1: 'Models.csv',
  file_2: 'Datasets.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 2025.6.0.
CREATE CONSTRAINT `username_Authors_uniq` IF NOT EXISTS
FOR (n: `Authors`)
REQUIRE (n.`username`) IS UNIQUE;
CREATE CONSTRAINT `model_id_Models_uniq` IF NOT EXISTS
FOR (n: `Models`)
REQUIRE (n.`model_id`) IS UNIQUE;
CREATE CONSTRAINT `paperswithcode_id_Dataset_uniq` IF NOT EXISTS
FOR (n: `Dataset`)
REQUIRE (n.`paperswithcode_id`) IS UNIQUE;

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
  MERGE (n: `Authors` { `username`: row.`username:ID(Author)` })
  SET n.`username` = row.`username:ID(Author)`
  SET n.`fullname` = row.`fullname`
  SET n.`avatar_url` = row.`avatar_url`
  SET n.`is_pro` = toFloat(trim(row.`is_pro`))
  SET n.`source` = row.`source`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`model_id` IN $idsToSkip AND NOT row.`model_id` IS NULL
CALL {
  WITH row
  MERGE (n: `Models` { `model_id`: row.`model_id` })
  SET n.`model_id` = row.`model_id`
  SET n.`pipeline_tag` = row.`pipeline_tag`
  SET n.`downloads` = toInteger(trim(row.`downloads`))
  SET n.`library_name` = row.`library_name`
  SET n.`likes` = row.`likes`
  SET n.`config` = row.`config`
  SET n.`downloads_log` = toFloat(trim(row.`downloads_log`))
  SET n.`Author_ID` = row.`Author_ID`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row
WHERE NOT row.`paperswithcode_id` IN $idsToSkip AND NOT row.`paperswithcode_id` IS NULL
CALL {
  WITH row
  MERGE (n: `Dataset` { `paperswithcode_id`: row.`paperswithcode_id` })
  SET n.`paperswithcode_id` = row.`paperswithcode_id`
  SET n.`dataset_id:ID(Dataset)` = row.`dataset_id:ID(Dataset)`
  SET n.`description` = row.`description`
  SET n.`citation` = row.`citation`
  SET n.`downloads` = toInteger(trim(row.`downloads`))
  SET n.`AuthorID` = row.`AuthorID`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Authors` { `username`: row.`username:ID(Author)` })
  MATCH (target: `Models` { `model_id`: row.`type` })
  MERGE (source)-[r: `AUTHORS_WITH_MODELS`]->(target)
  SET r.`username:ID(Author)` = row.`username:ID(Author)`
  SET r.`fullname` = row.`fullname`
  SET r.`avatar_url` = row.`avatar_url`
  SET r.`is_pro` = toFloat(trim(row.`is_pro`))
  SET r.`type` = row.`type`
  SET r.`source` = row.`source`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Authors` { `username`: row.`AuthorID` })
  MATCH (target: `Dataset` { `paperswithcode_id`: row.`dataset_id:ID(Dataset)` })
  MERGE (source)-[r: `AUTHORS_WITH_DATASETS`]->(target)
  SET r.`dataset_id:ID(Dataset)` = row.`dataset_id:ID(Dataset)`
  SET r.`description` = row.`description`
  SET r.`citation` = row.`citation`
  SET r.`paperswithcode_id` = row.`paperswithcode_id`
  SET r.`downloads` = toInteger(trim(row.`downloads`))
  SET r.`AuthorID` = row.`AuthorID`
} IN TRANSACTIONS OF 10000 ROWS;

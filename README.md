# AgileDb
AgileDb is an API and database technique designed to make databases agile. It enables the storage of various data types in a JSON field within SQL databases, offering flexibility without strict schema requirements. 
Postgres, mariadb and MSSQL are supported.

## Features
### Dynamic Structure
Save unstructured data in SQL databases by storing data types in a JSON field.
### Performance Optimization 
Improve performance by adding columns and indexes to the configuration file, which are then ported to the database upon API server start.
### Consistent API
The API interface remains consistent regardless of changes in the underlying database structure.
### Error-Free Expansion 
Easily add new fields to the data without causing API errors, allowing for seamless development and expansion.
### API
Currently supports REST methods GET, POST, PATCH, PUT, and DELETE.

## Usage Examples
### GET
Retrieves data similar to a SELECT query:
```json
{
    "type": "house",
    "columns": ["Address"]
}
```
Supports filtering with WHERE clauses:
```json
{
    "type": "house",
    "columns": ["Address"],
    "where": [{
        "Address": "Blubb%",
        "operator": "LIKE"
    }]
}
```
### POST
Inserts data into the database:
```json
{
    "type": "house",
    "data": {
        "id": 122131,
        "a": 3333,
        "Height": "4m",
        "Address": "Blubberdi",
        "changed": true
    }
}
```
### PUT
Updates existing data
```json
{
    "agile_id": "dc17d627-6041-4765-857b-5a95d9aa6f7c",
    "type": "house",
    "data": {
        "changed": true
    }
}
```
### DELETE
Deletes data from the database:
```json
{
    "type": "house",
    "agile_id": "dc17d627-6041-4765-857b-5a95d9aa6f7c"
}
```

## PATCH - Configuration
To enable raw SQL queries, modify the configuration file :
```json
{
    "sql": "SELECT data FROM agile_main"
}
```
ROADMAP: 
Prototype is in python. as the initial idea worked, solutions in golang, node.js, typescript and lua will be implemented.


Feel free to contribute or provide feedback!

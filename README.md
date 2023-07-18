# QADO SPARQL query validator
This repository contains a command-line tool to check if SPARQL
queries of your QADO triplestore return valid results from their
knowledge graphs.

## Execution
### Docker image
You can use this tool by pulling and executing a pre-build Docker Image:

```shell
docker pull wseresearch/qado-sparql-validator:latest
docker run --rm wseresearch/qado-sparql-validator:latest $FETCH_URL $UPDATE_URL
```

This tool work with triplestores which provide an HTTP API. Therefor, you
need to provide the endpoints used for fetching and updating data.

### Command-line tool
To use this tool on your CLI make sure you've installed a current [Rust environment](https://www.rust-lang.org/).
Then just clone this repository and run

```shell
cargo install --path .
```

Then you can use the tool by running `qado_sparql_validator $FETCH_URL $UPDATE_URL`.

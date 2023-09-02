use std::collections::{HashMap, LinkedList};
use std::string::ToString;
use std::thread;
use reqwest::blocking::Client;
use serde::Deserialize;
use threadpool::ThreadPool;
use chrono;
use clap::Parser;
use indicatif::ProgressBar;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Parser)]
struct Arguments {
    /// URL of the HTTP endpoint of your QADO triplestore
    fetch_url: String,
    /// URL of the HTTP endpoint used to update your QADO triplestore
    update_url: String
}

#[derive(Deserialize)]
struct Results {
    bindings: LinkedList<HashMap<String, HashMap<String, String>>>
}

/// Structure of the SPARQLResult for SELECT queries
#[derive(Deserialize)]
struct JsonResult {
    results: Results
}

/// Structure of a SPARQLResult for ASK queries
#[derive(Deserialize)]
struct JsonAskResult {
    boolean: bool
}

const CHECK_TRIPLE_STORES: [&'static str; 2] = ["https://dbpedia.org/sparql", "https://query.wikidata.org/sparql"];
const SELECT_QUERY: &str = "PREFIX qado: <http://purl.com/qado/ontology.ttl#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
select ?query ?text where {?question a ?class ; qado:hasSparqlQuery ?query . ?class rdfs:subClassOf qado:Question .\
?query a qado:Query ; qado:hasQueryText ?text .} ORDER BY ?query";

/// Evaluating all found queries
///
/// # Arguments
/// * `bindings`: bindings of the SPARQLResult+JSON object returned from the QADO triplestore
fn check_queries(bindings: LinkedList<HashMap<String, HashMap<String, String>>>,
                 update_url: String) {
    let threads = match thread::available_parallelism() {
        Ok(threads) => threads.get(),
        Err(_) => 1
    };

    println!("Using {} threads...", threads);

    let pool = ThreadPool::new(threads);
    let bar_arc = Arc::new(Mutex::new(ProgressBar::new(bindings.len() as u64)));

    for binding in bindings.iter() {
        let query_id = binding["query"]["value"].clone();
        let query_text = binding["text"]["value"].clone();
        let update_triple_store = update_url.clone();

        let bar_handle = Arc::clone(&bar_arc);

        pool.execute(move|| {
            bar_handle.lock().unwrap().inc(1);
            evaluate_triple_stores(query_id, query_text, update_triple_store);
        });
    }

    pool.join();
    bar_arc.lock().unwrap().finish();
}

/// Create the insert query for the evaluation results of a SPARQL query
///
/// # Arguments
/// * `query_id` - identifier of the evaluated SPARQL query object
/// * `endpoint` - HTTP endpoint of the knowledge graph related to the query
/// * `property` - property name of the evaluation result
/// * `valid` - **false** if the knowledge returned an empty response else **true**
/// * `update_triple_store` - HTTP endpoint of the evaluated QADO dataset
fn generate_insert_query(query_id: String, endpoint: &str, property: &str, valid: bool,
                         update_triple_store: String) {
    let time = chrono::offset::Utc::now().format("%FT%T");

    let mut query = format!("PREFIX qado: <http://purl.com/qado/ontology.ttl#> \
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
    insert {{ <{query_id}> qado:hasSPARQLCheck [ \
        a qado:SPARQLCheck ; \
        qado:{property} \"{time}\"^^xsd:dateTime ].");

    if valid {
        query = format!("{query} <{query_id}> qado:correspondsToKnowledgeGraph <{endpoint}> .");
    }

    query = format!("{query} }} where {{}}");

    let client = Client::new();
    client.post(update_triple_store).query(&[("update", query)]).send().expect("Query failed!");
}

/// Validates a SPARQL query and stores the results in the corresponding QADO triplestore
///
/// # Arguments
/// * `query_id` - identifier for the evaluated qado:Query object
/// * `query_text` - SPARQL query
/// * `update_triple_store` - HTTP endpoint for posting UPDATE queries for the QADO triplestore
fn evaluate_triple_stores(query_id: String, query_text: String, update_triple_store: String) {
    let mut updated: bool = false;

    // test all listed triplestores
    for triplestore in CHECK_TRIPLE_STORES.iter() {
        let client = Client::builder().timeout(Duration::new(90, 0)).
            build().expect("Client build failed");

        let response = client.get(triplestore.to_string()).query(
            &[("query", query_text.as_str())]).header("Accept", "application/json"
        ).send();

        match response {
            Ok(mut http_resp) => {
                if http_resp.status().is_success() {
                    let mut http_body: Vec<u8> = vec![];
                    http_resp.copy_to(&mut http_body).expect("Copy of body failed");

                    let data_result = serde_json::from_slice::<JsonResult>(
                        http_body.clone().as_slice());

                    match data_result {
                        Ok(data) => {
                            if data.results.bindings.len() > 0 {
                                generate_insert_query(query_id.clone(), triplestore,
                                                      "testedSuccessfullyAt", true,
                                                      update_triple_store.clone());
                            } else {
                                generate_insert_query(query_id.clone(), triplestore,
                                                      "didNotWorkAt", true,
                                                      update_triple_store.clone());
                            }

                            updated = true;
                            break;
                        },
                        Err(_) => {
                            let ask_body = serde_json::from_slice::<JsonAskResult>(
                                http_body.as_slice());

                            match ask_body {
                                Ok(body) => {
                                    if body.boolean {
                                        generate_insert_query(query_id.clone(), triplestore,
                                        "testedSuccessfullyAt", true,
                                                              update_triple_store.clone());
                                    }
                                    else {
                                        generate_insert_query(query_id.clone(), triplestore,
                                        "didNotWorkAt", false,
                                                              update_triple_store.clone())
                                    }

                                    updated = true;
                                    break;
                                },
                                Err(error) => println!("Invalid answer provided: {}", error)
                            }
                        }
                    }
                }
            },
            Err(error) => println!("Request failed! {}", error.to_string())
        }
    }

    if !updated {
        generate_insert_query(query_id, &String::new(), "didNotWorkAt", false,
                              update_triple_store.clone());
    }
}

fn main() {
    let args = Arguments::parse();

    let client = Client::builder().timeout(Duration::from_secs(60)).build().unwrap();
    let response = client.get(args.fetch_url).header(
        "Accept", "application/sparql-results+json").query(&[("query", SELECT_QUERY)]).send();

    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                let body_json : Result<JsonResult, _> = resp.json();

                match body_json {
                    Ok(body) => {
                        check_queries(body.results.bindings, args.update_url);
                    }
                    Err(e) => {
                        println!("{}", e);
                    }
                }
            }
            else {
                println!("{}", resp.status());
            }
        },
        Err(err) => {
            println!("{}", err);
        }
    }
}

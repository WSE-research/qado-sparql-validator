use std::collections::{HashMap, LinkedList};
use std::string::ToString;
use std::{thread, env};
use reqwest::blocking::Client;
use serde::Deserialize;
use threadpool::ThreadPool;
use chrono;

#[derive(Deserialize)]
struct Results {
    bindings: LinkedList<HashMap<String, HashMap<String, String>>>
}

#[derive(Deserialize)]
struct JsonResult {
    results: Results
}

#[derive(Deserialize)]
struct JsonAskResult {
    boolean: bool
}

const SELECT_QUERY: &str = "PREFIX qado: <http://purl.com/qado/ontology.ttl#> select \
?query ?text where {?question a qado:Question ; qado:hasSparqlQuery ?query .?query a qado:Query ;\
qado:hasQueryText ?text .} ORDER BY ?query";

fn check_queries(bindings: LinkedList<HashMap<String, HashMap<String, String>>>) {
    let threads = match thread::available_parallelism() {
        Ok(threads) => threads.get(),
        Err(_) => 1
    };

    println!("Using {} threads...", threads);

    let pool = ThreadPool::new(threads);

    for binding in bindings.iter() {
        let query_id = binding["query"]["value"].clone();
        let query_text = binding["text"]["value"].clone();

        pool.execute(move|| {
            let args: Vec<String> = env::args().collect();
            evaluate_triple_stores(query_id, query_text, args[2].clone());
        });
    }

    pool.join();
}

fn generate_insert_query(query_id: String, endpoint: &String, property: &str, valid: bool,
                         update_triple_store: String) {
    let time = chrono::offset::Utc::now().format("%FT%T");

    let sparql_query_check = format!("{query_id}-check");

    let mut query = format!("PREFIX qado: <http://purl.com/qado/ontology.ttl#> \
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
    insert {{ <{query_id}> qado:hasSPARQLCheck <{sparql_query_check}> . \
    <{sparql_query_check}> a qado:SPARQLCheck . \
    <{sparql_query_check}> qado:{property} \"{time}\"^^xsd:dateTime .");

    if valid {
        query = format!("{query} <{query_id}> qado:correspondsToKnowledgeGraph <{endpoint}> .");
    }

    query = format!("{query} }} where {{}}");

    let client = Client::new();
    client.post(update_triple_store).query(&[("update", query)]).send().expect("Query failed!");
}

fn evaluate_triple_stores(query_id: String, query_text: String, update_triple_store: String) {
    let mut updated: bool = false;
    let check_triple_stores = LinkedList::from([
        "https://dbpedia.org/sparql".to_string(), "https://query.wikidata.org/sparql".to_string()]);

    for triplestore in check_triple_stores.iter() {
        let client = Client::new();
        let response = client.get(triplestore).query(
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
    let args: Vec<String> = env::args().collect();

    if args.len() == 3 {
        let fetch_triple_store = args[1].as_str();

        let client = Client::new();
        let response = client.get(fetch_triple_store).header(
            "Accept", "application/json").query(&[("query", SELECT_QUERY)]).send().unwrap();

        let body: JsonResult = response.json().unwrap();

        check_queries(body.results.bindings);
    }
    else {
        println!("Run this command by calling qado_expander [FETCH_URL] [UPDATE_URL]");
        println!("FETCH_URL\t-\tHTTP endpoint of your QADO triplestore to fetch the SPARQL queries");
        println!("UPDATE_URL\t-\tHTTP endpoint of your QADO triplestore to post SPARQL UPDATE queries");
    }
}

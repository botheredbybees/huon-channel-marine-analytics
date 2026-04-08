import requests
import json
import urllib.parse

# Approximation for Storm Bay, Huon, Channel, Derwent:
# Roughly: South: -43.6, North: -42.7, West: 146.8, East: 147.8
# The DAP api takes a bbox in format: "west longitude,south latitude,east longitude,north latitude"
bbox = "146.8,-43.6,147.8,-42.7"

def search_dap(keywords, keyword_operator="AND"):
    url = "https://data.csiro.au/dap/ws/v2/collections.json"
    params = {}
    
    if keywords:
        q_string = f" {keyword_operator} ".join([f'"{k}"' for k in keywords])
        # Add 'model' or 'hydrodynamic' to narrow down
        params["q"] = f'({q_string}) AND (model OR hydrodynamic)'

    print(f"Querying DAP API with params: {params}")
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return

    data = response.json()
    collections = data.get("dataCollections", [])
    print(f"\nFound {len(collections)} collections:")

    total_size_estimate_mb = 0

    for c in collections:
        title = c.get("title", "No Title")
        pub_date = c.get("published", "")
        print(f"\n- Title: {title}")
        print(f"  Published: {pub_date}")
        
        # We need to get the specific collection to see the files and size
        collection_url = c.get("self", "")
        if collection_url:
            c_resp = requests.get(collection_url.replace(".xml", ".json", 1) if ".xml" in collection_url else f"{collection_url}.json")
            if c_resp.status_code == 200:
                c_data = c_resp.json()
                data_collection = c_data.get("dataCollection", {})
                
                # Try to hit the data endpoint to check file size
                data_url = data_collection.get("data", "")
                if data_url:
                    files_resp = requests.get(data_url.replace(".xml", ".json", 1) if ".xml" in data_url else f"{data_url}.json")
                    if files_resp.status_code == 200:
                        files_data = files_resp.json()
                        files = files_data.get("files", [])
                        coll_size = sum([f.get("size", 0) for f in files])
                        coll_size_mb = coll_size / (1024 * 1024)
                        total_size_estimate_mb += coll_size_mb
                        print(f"  Files: {len(files)} (Approx {coll_size_mb:.2f} MB)")
                    else:
                        print(f"  Data URL error: {files_resp.status_code}")
                # Print summary
                description = data_collection.get("description", "")
                if description:
                    print(f"  Description Snippet: {description[:100]}...")
            else:
                 print(f"  Details URL error: {c_resp.status_code}")

    print(f"\nTotal estimated size of data in these collections: {total_size_estimate_mb / 1024:.2f} GB")

if __name__ == "__main__":
    search_keywords = ["Storm Bay", "Huon", "D'Entrecasteaux", "Derwent"]
    search_dap(search_keywords, keyword_operator="OR")

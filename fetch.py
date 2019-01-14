#!/usr/bin/env python

import sys
from ipdb import set_trace as st

from elasticsearch import Elasticsearch
from requests import post
from json import dumps
from itertools import chain


records = []
ucsd_request_body = {
    "sort": {
        "date_created": {
            "order": "asc"
        }
    },
    "size": 10000,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "UCSD",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "UC San Diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "UC San Diego Library Digital Collections",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "Scripps Institution of Oceanography",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "Scripps Institute of Oceanography",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "University of California San Diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "Univ of california san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "University of CA San Diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "university of california at san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "university california san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "univ of california at san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "univ california san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "univ calif san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "california univ san diego",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "san diego supercomputer center",
                                    "type": "phrase"
                                }
                            },
                            {
                                "multi_match": {
                                    "fields": [
                                        "contributors",
                                        "publishers",
                                        "affiliations",
                                        "funders",
                                        "title",
                                        "hosts"
                                    ],
                                    "query": "qualcomm institute",
                                    "type": "phrase"
                                }
                            },
                            {
                                "match_phrase": {
                                    "tags": "cdl.ucsd"
                                }
                            },
                            {
                                "match_phrase": {
                                    "tags": "Scripps institution of oceanography"
                                }
                            },
                            {
                                "term": {
                                    "source": "UC San Diego Library"
                                }
                            }
                        ]
                    }
                }
            ],
            "must": [
                {
                    "query_string": {
                        "query": "*"
                    }
                }
            ]
        }
    }
}

asapbio_request_body = {
    "sort": {
        "date_created": {
            "order": "asc"
        }
    },
    "size": 5000,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "type": "preprint"
                                }
                            },
                        ]
                    }
                }
            ],
            "must": [
                {
                    "query_string": {
                        "query": "*"
                    }
                }
            ]
        }
    }
}


# Fetch records in recent half
request_body  = asapbio_request_body

request_body['sort']['date_created']['order'] = 'desc'

record_dict = {}

total = None

while total is None or len(records) < total:
    if total is not None:
        print(f"{len(record_dict)} unique of {len(records)} retrieved from {total} total records.")
        request_body['search_after'] = records[-1]['sort']
    else:
        print("Beginning record retrieval")
    response = None
    print("Making request.")
    response = post(
        'https://share.osf.io/api/v2/search/creativeworks/_search',
        headers={
            'Content-Type': 'application/json'
        },
        data=dumps(request_body)
    ).json()
    print("Request successful.")
    try:
        records = records + response['hits']['hits']
        total = response['hits']['total']
        record_dict = {}

        for record in records:
            record_dict[record['_id']] = record

        with open('./asapbio.json', 'w') as file:
            file.write(dumps(records))
    except:
        print(response.keys())
        break

print('# Press ctrl-d to exit without writing the data to a file')
import ipdb; ipdb.set_trace()


sys.exit()

import ipdb; ipdb.set_trace()
request_body['sort']['date_created']['order'] = 'asc'

res = None

try:
    res = post(
        'https://share.osf.io/api/v2/search/creativeworks/_search',
        headers={
            'Content-Type': 'application/json'
        },
        data=dumps(request_body)
    )
except:
    import ipdb; ipdb.set_trace()


# Fetching records complete
###############################################################################
# Deduplicate fetched records


record_dict = {}

for record in records:
    record_dict[record['_id']] = record

print(len(record_dict))

import sys
sys.exit()

# Records deduplicated
###############################################################################
# Upload records to UCSD elastic cluster


ucsd_elastic_cluster = Elasticsearch(
    ['https://dev-labs.cos.io'],
    use_ssl=True,
    retry_on_timeout=True
)


for record in records:

    res = ucsd_elastic_cluster.index(
        index="records",
        doc_type="record",
        id=record["_id"],
        body=record["_source"]
    )



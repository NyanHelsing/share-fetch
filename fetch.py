#!/usr/bin/env python


from elasticsearch import Elasticsearch
from requests import post
from json import dumps
from itertools import chain


records = []
request_body = {
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


# Fetch records in recent half

request_body['sort']['date_created']['order'] = 'desc'


try:
    records = records + post(
        'https://share.osf.io/api/v2/search/creativeworks/_search',
        headers={
            'Content-Type': 'application/json'
        },
        data=dumps(request_body)
    ).json()['hits']['hits']
except:
    import ipdb; ipdb.set_trace()


# Fetch reocords in early half

request_body['sort']['date_created']['order'] = 'asc'

try:
    records = records + post(
        'https://share.osf.io/api/v2/search/creativeworks/_search',
        headers={
            'Content-Type': 'application/json'
        },
        data=dumps(request_body)
    ).json()['hits']['hits']
except:
    import ipdb; ipdb.set_trace()


# Fetching records complete
###############################################################################
# Deduplicate fetched records


record_dict = {}

for record in records:
    record_dict[record['_id']] = record

print(record_dict.values())



# Records deduplicated
###############################################################################
# Upload records to UCSD elastic cluster

ucsd_elastic_cluster = Elasticsearch([
    'https://elastic:pJBjKf1HFVXaJYKEABXicHx4@https://a99f2969ba11fbde063dfcd46130d10e.us-east-1.aws.found.io:9243'
],
    use_ssl=True
)



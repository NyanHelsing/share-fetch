#!/usr/bin/env python

import sys
from ipdb import set_trace as st

from elasticsearch import Elasticsearch
from requests import post
from json import dumps
from itertools import chain

class FailError(Exception):
    pass

class UndefinedResponse():

    @property
    def status_code(self):
        return 0

records = []
ucsd_request_body = {
    "sort": {
        "date_created": {
            "order": "asc"
        }
    },
    "size": 1000,
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
    "size": 1,
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
request_body = ucsd_request_body

request_body['sort']['date_created']['order'] = 'desc'

record_dict = {}

total = None

ucsd_elastic_cluster = Elasticsearch(
    ['https://dev-labs.cos.io'],
    use_ssl=True,
    retry_on_timeout=True
)
#with open('./asapbio.json', 'w+') as file:
#    file.write("[")

while total is None or len(record_dict) < total:
    if total is not None:
        print(f"{len(record_dict)} unique of {len(records)} retrieved from {total} total records.")
        if not records[-1].get('sort'):
            break
        print(records[-1])
        print(records[-1]['sort'])
        request_body['search_after'] = records[-1]['sort']
        print(request_body)
        records = []
    else:
        print("Beginning record retrieval")
    response = None

    response_obj = UndefinedResponse()
    request_body['size'] = 1000

    try:
        retry = False
        while not response_obj.status_code == 200:

            print("Making request.")
            print(request_body["size"])

            try:
                response_obj = post(
                    'https://share.osf.io/api/v2/search/creativeworks/_search',
                    headers={
                        'Content-Type': 'application/json'
                    },
                    data=dumps(request_body)
                )
            except:
                retry = True

            if not response_obj.status_code == 200 or retry:

                retry = False

                if request_body['size'] <= 5:
                    request_body['size'] = request_body['size'] - 1
                else:
                    request_body['size'] = request_body['size'] / 10

                if request_body['size'] == 0:
                    raise FailError()

    except FailError:
        break

    response = response_obj.json()
    print("Request successful.")
    try:
        records = response['hits']['hits']
        total = response['hits']['total']

        #with open('./asapbio.json', 'a+') as file:
            #file.write("\n")
        for record in records:
            #record_dict[record['_id']] = record
            #file.write(dumps(record))
            #file.write(",")

            try:

                pass

                res = ucsd_elastic_cluster.index(
                    index="records",
                    doc_type="record",
                    id=record["_id"],
                    body=record["_source"]
                )

            except:
                pass




    except:
        break

#with open('./asapbio.json', 'a+') as file:
#    f.seek(-1, os.SEEK_CUR)
#    file.write("\n]")

#print('# Press ctrl-d to exit without writing the data to a file')
#import ipdb; ipdb.set_trace()


#sys.exit()

# The following is kept for postereity.
###############################################################################

#import ipdb; ipdb.set_trace()
#request_body['sort']['date_created']['order'] = 'asc'

#res = None

#try:
#    res = post(
#        'https://share.osf.io/api/v2/search/creativeworks/_search',
#        headers={
#            'Content-Type': 'application/json'
#        },
#        data=dumps(request_body)
#    )
#except:
#    import ipdb; ipdb.set_trace()


# Fetching records complete
###############################################################################
# Deduplicate fetched records


#record_dict = {}

#for record in records:
#    record_dict[record['_id']] = record

#print(len(record_dict))
#
#import sys
#sys.exit()

# Records deduplicated
###############################################################################
# Upload records to UCSD elastic cluster


#ucsd_elastic_cluster = Elasticsearch(
#    ['https://dev-labs.cos.io'],
#    use_ssl=True,
#    retry_on_timeout=True
#)


#for record in records:

#    res = ucsd_elastic_cluster.index(
#        index="records",
#        doc_type="record",
#        id=record["_id"],
#        body=record["_source"]
#    )



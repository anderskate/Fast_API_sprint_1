from typing import Optional, List
from pydantic import parse_obj_as
from operator import itemgetter
from pydantic.typing import Type


def get_sort_for_elastic(sort: Optional[str] = None) -> dict:
    res = {}
    if sort:
        if sort[0] == '-':
            order = 'desc'
            field = sort[1:]
        else:
            order = 'asc'
            field = sort
        res['sort'] = {field: {"order": order}}
    return res


def get_genres_filter_for_elastic(genres: Optional[List[str]] = None) -> dict:
    res = {}
    if genres:
        should = [{"match": {"genres.id": genre} for genre in genres}]
        res["query"] = {
            "nested": {
                "path": "genres",
                "query": {
                    "bool": {"should": should}
                }
            }
        }
    return res


def get_search_body_for_movies(query: str, fields_for_search: Optional[List[str]] = None) -> dict:
    if fields_for_search is None:
        fields_for_search = ["title", "actors_names"]
    return {
        "query": {
            "multi_match": {
                "query": query,
                "fields": fields_for_search
            }
        }
    }


def parse_objects(doc: dict, schema: Type) -> List:
    if doc and doc.get('hits'):
        return parse_obj_as(List[schema], list(map(itemgetter('_source'), doc['hits'].get('hits', []))))
    return []

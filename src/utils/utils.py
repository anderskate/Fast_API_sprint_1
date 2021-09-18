from operator import itemgetter
from typing import List, Optional

from pydantic import parse_obj_as
from pydantic.main import ModelMetaclass

from src.models.movie import ALLOWED_SORT_FIELDS, FIELDS_FOR_SEARCH


def get_movies_sorting_for_elastic(sort_field: Optional[str] = None) -> dict:
    res = {}
    if sort_field:
        if sort_field[0] == "-":
            order = "desc"
            field = sort_field[1:]
        else:
            order = "asc"
            field = sort_field
        if field in ALLOWED_SORT_FIELDS:
            res["sort"] = {field: {"order": order}}
    return res


def get_genres_filter_for_elastic(genres: Optional[List[str]] = None) -> dict:
    res = {}
    if genres:
        should = [{"match": {"genres.id": genre} for genre in genres}]
        res["query"] = {
            "nested": {"path": "genres", "query": {"bool": {"should": should}}}
        }
    return res


def get_search_body_for_movies(
    query: str, fields_for_search: Optional[List[str]] = None
) -> dict:
    if fields_for_search is None:
        fields_for_search = FIELDS_FOR_SEARCH
    return {"query": {"multi_match": {"query": query, "fields": fields_for_search}}}


def parse_objects(doc: dict, schema: ModelMetaclass) -> List:
    if doc and doc.get("hits"):
        return parse_obj_as(
            List[schema], list(map(itemgetter("_source"), doc["hits"].get("hits", [])))
        )
    return []

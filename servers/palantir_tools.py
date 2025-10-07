from mcp.server.fastmcp.server import FastMCP, Image
from PIL import Image as PILImage
import httpx
import pyodbc
import os
import requests
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
import json
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)
global mcp 
mcp = FastMCP("MyApp")
endpoint = os.getenv("PALANTIR_ENDPOINT")
ontology_id = os.getenv("ONTOLOGY_ID")

class ListObjectTypesParams(BaseModel):
    limit: int = Field(default=100, description="Maximum number of objects to return")
    pagesize: int = Field(default=100, description="Number of items per page")
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    
class GetObjectTypeParams(BaseModel):
    object_type_id: str = Field(..., description="ID of the object type to retrieve")
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")

class ListObjectsParams(BaseModel):
    limit: int = Field(default=100, description="Maximum number of objects to return")
    pagesize: int = Field(default=100, description="Number of items per page")
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    object_type_id: str = Field(..., description="ID of the object type to list Objects From")
    properties: Optional[List[str]] = Field(default=None, description="List of properties to include in the response")
    sort: Optional[Dict[str, Any]] = Field(default=None, description="Sorting criteria for the results")

class SearchObjectsParams(BaseModel):
    limit: int = Field(default=100, description="Maximum number of objects to return")
    pagesize: int = Field(default=100, description="Number of items per page")
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    object_type_id: str = Field(..., description="ID of the object type to search")
    properties: Optional[List[str]] = Field(default=None, description="List of properties to include in the response")
    sort: Optional[Dict[str, Any]] = Field(default=None, description="Sorting criteria for the results")
    query: List[Dict[str, Any]] = Field(..., description="Search query conditions")

class AggregateObjectsParams(BaseModel):
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    object_type_id: str = Field(..., description="ID of the object type to aggregate")
    groupby: Optional[List[Dict[str,Any]]] = Field(default=None, description="Property to group by")
    aggregation: List[Dict[str,Any]] = Field(default=None, description="Type of aggregation (e.g., count, sum, avg)")
    query: Optional[List[Dict[str, Any]]] = Field(default=None, description="Filter conditions for aggregation")

class GetObjectParams(BaseModel):
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    object_type_id: str = Field(..., description="ID of the object type to retrieve object from")
    primary_key: str = Field(..., description="Primary Key of the object to retrieve")

class ApplyActionParams(BaseModel):
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    action_id: str = Field(..., description="ID of the action to apply")
    inputs: Dict[str, Any] = Field(default=None, description="Input parameters for the action")

class ApplyBatchActionsParams(BaseModel):
    ontology: str = Field(default=ontology_id, description="Ontology to filter by")
    action_id: str = Field(..., description="ID of the action to apply")
    inputs: List[Dict[str, Any]] = Field(default=None, description="List of input parameters for the action")

def _get_token():
    url = os.getenv("PALANTIR_SECURITY_ENDPOINT")
    data = {
       "grant_type" : "client_credentials",
       "client_id" : os.getenv("CLIENT_ID"),
       "client_secret":os.getenv("CLIENT_SECRET")
    }
    response = requests.post(url, data = data, verify=False)
    bearer_token_response = json.loads(response.content.decode("utf-8"))
    return bearer_token_response["access_token"]

def _get_headers():
    token = _get_token()
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    return headers

@mcp.tool()
def list_object_types(params: ListObjectTypesParams):

    """List Object Types from a given ontology."""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objectTypes"
        headers = _get_headers()
        parameters = dict()
        parameters["pageSize"] = params.pagesize
        results = []
        while len(results)<params.limit:
            response = requests.get(api_endpoint, headers=headers, params=parameters, verify=False)
            response_json = json.loads(response.content.decode("utf-8"))
            results.extend(response_json["data"])
            if "nextPageToken" in response_json:
                parameters["pageToken"] = response_json["nextPageToken"]
            else:
                break
        if len(results)>params.limit:
            results = results[:params.limit]
        return {
                "success": True,
                "message": f"Retrieved {len(results)} Object Types",
                "items": results,
                "total": len(results),
                "limit": params.limit,
                "pageSize": params.pagesize
            }
    except Exception as e:
        logger.error(f"Error listing Object Types: {str(e)}")
        return {
            "success": False,
            "message": f"Error listing Object Types: {str(e)}",
            "items": [],
            "total": 0,
            "limit": params.limit,
            "pageSize": params.pagesize,
        }

@mcp.tool()
def get_object_type(params: GetObjectTypeParams):

    """Retrieve Object Type from a given ontology."""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objectTypes/{params.object_type_id}"
        headers = _get_headers()
        response = requests.get(api_endpoint, headers=headers, verify=False)
        response_json = json.loads(response.content.decode("utf-8"))
        return {
                "success": True,
                "message": f"Retrieved {response_json["displayName"]} Object Types",
                "item": response_json
            }
    except Exception as e:
        logger.error(f"Error Retrieving {params.object_type_id} Object Type: {str(e)}")
        return {
            "success": False,
            "message": f"Error Retrieving {params.object_type_id} Object Type: {str(e)}",
            "item": None,
        }

@mcp.tool()      
def list_objects(params: ListObjectsParams):

    """List Objects from a given Object Type."""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objects/{params.object_type_id}"
        headers = _get_headers()
        parameters = dict()
        parameters["pageSize"] = params.pagesize
        if params.properties:
            parameters["select"] = params.properties
        if params.sort:
            orderby = [f"p.{k}:{v}" for k,v in params.sort.items() if k in params.properties or not params.properties]
            parameters["orderBy"] = ",".join(orderby)
        results = []
        while len(results)<params.limit:
            response = requests.get(api_endpoint, headers=headers, params=parameters, verify=False)
            response_json = json.loads(response.content.decode("utf-8"))
            
            if response_json.get("data"):
                results.extend(response_json["data"])
            if "nextPageToken" in response_json:
                parameters["pageToken"] = response_json["nextPageToken"]
            else:
                break
        if len(results)>params.limit:
            results = results[:params.limit]
        return {
                "success": True,
                "message": f"Retrieved {len(results)} Objects",
                "items": results,
                "total": len(results),
                "limit": params.limit,
                "pageSize": params.pagesize
            }
    except Exception as e:
        logger.error(f"Error listing Objects: {str(e)}")
        return {
            "success": False,
            "message": f"Error listing Objects: {str(e)}",
            "items": [],
            "total": 0,
            "limit": params.limit,
            "pageSize": params.pagesize,
        } 

@mcp.tool()  
def get_object(params: GetObjectParams):

    """Retrieve Object from a given Object Type."""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objectTypes/{params.object_type_id}/{params.primary_key}"
        headers = _get_headers()
        response = requests.get(api_endpoint, headers=headers, verify=False)
        response_json = json.loads(response.content.decode("utf-8"))
        return {
                "success": True,
                "message": f"Retrieved {response_json["__primaryKey"]} Object",
                "item": response_json
            }
    except Exception as e:
        logger.error(f"Error Retrieving {params.primary_key} Object: {str(e)}")
        return {
            "success": False,
            "message": f"Error Retrieving {params.primary_key} Object: {str(e)}",
            "item": None,
        }

def _non_boolean_query_conditions(conditions:dict)->dict:
    query=dict()
    query["type"] = list(conditions.keys())[0]
    query["field"] = (list(conditions.values())[0])[0]
    query["value"] = (list(conditions.values())[0])[1]
    return query

def _boolean_query_conditions(conditions:dict)->dict:
    query=dict()
    query["type"] = list(conditions.keys())[0]
    query["value"] = list()
    values = list(conditions.values())[0]
    for v in values:
        if list(v.keys())[0] in ["and","or","not"]:
            query["value"].append(_boolean_query_conditions(v))
        else:
            query["value"].append(_non_boolean_query_conditions(v))
    return query

def _construct_filter_query(conditions: List[Dict[str, Any]],properties:list,sort:Dict,pagesize:int) -> dict:
    query = dict()
    if pagesize:
        query["pageSize"] = pagesize
    if properties and len(properties)>0:
        query["select"] = properties
    if sort:
        orderby = [{"field":k,"direction":v} for k,v in sort.items() if k in properties or not properties or len(properties)==0]
        query["orderBy"] = {"fields":orderby}
    for condition in conditions:
        if list(condition.keys())[0] in ["and","or","not"]:
            query["where"] = _boolean_query_conditions(condition)
        else:
            query["where"] = _non_boolean_query_conditions(condition)
    return query

def _construct_aggregation_query(aggregations: List[Dict[str, Any]]):
    query=dict()
    query['aggregations']=list()
    for agg in aggregations:
        agg_type = list(agg.keys())[0]
        if agg_type == 'count':
            name = (list(agg.values())[0])[0] if list(agg.values())[0] and len(list(agg.values())[0])>0 else None
            query["aggregations"].append({"type":agg_type,"name":name} if name else {"type":agg_type})
        else:
            agg_property = (list(agg.values())[0])[0]
            name = (list(agg.values())[0])[1] if len(list(agg.values())[0])>1 else None
            query["aggregations"].append({"type":agg_type,"field":f"properties.{agg_property}","name":name} if name else {"type":agg_type,"field":f"properties.{agg_property}"})
    return query

def _construct_groupby_query(groupby: List[Dict[str, Any]]):
    query=dict()
    query['groupBy']=list()
    for group in groupby:
        groupby_type = list(group.keys())[0]
        if groupby_type == 'ranges':
            groupby_property = group[groupby_type][0]
            ranges = group[groupby_type][1]
            op = [{"startValue":r[0],"endValue":r[1]} for r in ranges]
            query["groupBy"].append({"type":groupby_type,"field":groupby_property,"ranges":op})
        elif groupby_type == 'exact':
            groupby_property = group[groupby_type][0]
            query["groupBy"].append({"type":groupby_type,"field":groupby_property})
        elif groupby_type == 'duration':
            groupby_property = group[groupby_type][0]
            duration = group[groupby_type][1]
            query["groupBy"].append({"type":groupby_type,"field":groupby_property,"duration":duration})
        elif groupby_type == 'fixedWidth':
            groupby_property = group[groupby_type][0]
            width = group[groupby_type][1]
            query["groupBy"].append({"type":groupby_type,"field":groupby_property,"fixedWidth":width})
        else:
            continue
    return query

@mcp.tool()
def search_objects(params: SearchObjectsParams):

    """Search Objects from a given Object Type."""
    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objects/{params.object_type_id}/search"
        headers = _get_headers()
        if not params.properties:
            params.properties = list()
        if not params.sort:
            params.sort = dict()
        payload = _construct_filter_query(params.query,params.properties,params.sort,params.pagesize)
        print(payload)
        results = []
        while len(results)<params.limit:
            response = requests.post(api_endpoint, headers=headers, json=payload, verify=False)
            response_json = json.loads(response.content.decode("utf-8"))
            
            if response_json.get("data"):
                results.extend(response_json["data"])
            if "nextPageToken" in response_json:
                payload["pageToken"] = response_json["nextPageToken"]
            else:
                break
        if len(results)>params.limit:
            results = results[:params.limit]
        return {
                "success": True,
                "message": f"Retrieved {len(results)} Objects",
                "items": results,
                "total": len(results),
                "limit": params.limit,
                "pageSize": params.pagesize
            }
    except Exception as e:
        logger.error(f"Error searching Objects: {str(e)}")
        return {
            "success": False,
            "message": f"Error searching Objects: {str(e)}",
            "items": [],
            "total": 0
        }

@mcp.tool()
def aggregate_objects(params: AggregateObjectsParams):

    """Aggregate Objects from a given Object Type."""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/objects/{params.object_type_id}/aggregate"
        headers = _get_headers()
        payload = dict()
        if params.groupby and len(params.groupby)>0:
            payload["groupBy"] = _construct_groupby_query(params.groupby)["groupBy"]
        if params.aggregation and len(params.aggregation)>0:
            payload["aggregation"] = _construct_aggregation_query(params.aggregation)["aggregations"]
        if params.query and len(params.query)>0:
            payload["where"] = _construct_filter_query(params.query,[],{},None)["where"]
        response = requests.post(api_endpoint, headers=headers, json=payload, verify=False)
        response_json = json.loads(response.content.decode("utf-8"))
        return {
                "success": True,
                "message": f"Aggregated {len(response_json.get('data',[]))} Objects",
                "items": response_json.get("data",[]),
                "total": len(response_json.get("data",[]))
            }
    except Exception as e:
        logger.error(f"Error aggregating Objects: {str(e)}")
        return {
            "success": False,
            "message": f"Error aggregating Objects: {str(e)}",
            "items": [],
            "total": 0
        }

@mcp.tool()
def apply_action(params: ApplyActionParams):

    """Apply Action on a given ontology"""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/actions/{params.action_id}/apply"
        headers = _get_headers()
        payload = dict()
        if params.inputs:
            payload["parameters"] = params.inputs
        response = requests.post(api_endpoint, headers=headers, json=payload, verify=False)
        response_json = json.loads(response.content.decode("utf-8"))
        return {
                "success": True,
                "message": f"Applied Action {params.action_id}",
                "item": response_json
            }
    except Exception as e:
        logger.error(f"Error applying Action {params.action_id}: {str(e)}")
        return {
            "success": False,
            "message": f"Error applying Action {params.action_id}: {str(e)}",
            "item": None
        }
    
@mcp.tool()
def apply_batch_actions(params: ApplyBatchActionsParams):

    """Apply Batch Actions on a given ontology"""

    try:
        api_endpoint = f"{endpoint}api/v2/ontologies/{params.ontology}/actions/{params.action_id}/applyBatch"
        headers = _get_headers()
        payload = dict()
        if params.inputs and len(params.inputs)>0:
            payload["requests"] = [{"parameters":i} for i in params.inputs]
        
        response = requests.post(api_endpoint, headers=headers, json=payload, verify=False)
        response_json = json.loads(response.content.decode("utf-8"))
        return {
                "success": True,
                "message": f"Applied Batch Action {params.action_id}",
                "items": response_json
            }
    except Exception as e:
        logger.error(f"Error applying Batch Action {params.action_id}: {str(e)}")
        return {
            "success": False,
            "message": f"Error applying Batch Action {params.action_id}: {str(e)}",
            "items": []
        }
    
if __name__ == "__main__":
    mcp.run(transport="stdio")
   


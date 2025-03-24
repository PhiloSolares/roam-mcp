from typing import Dict, List, Any, Optional, Union
import json
import httpx
import re
import os
import sys
from datetime import datetime
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("roam-helper")

# Constants for API endpoints
ROAM_API_BASE = "https://api.roamresearch.com/api/graph"


class PreserveAuthSession(httpx.Client):
    def rebuild_auth(self, prepared_request, response):
        return


async def make_roam_request(method: str,
                            endpoint: str,
                            api_token: str,
                            graph_name: str,
                            json_data: Optional[Dict] = None) -> Dict:
    """Make an authenticated request to the Roam Research API."""
    print(f"Making request to {endpoint} with token starting with: {api_token[:5]}... and graph: {graph_name}", file=sys.stderr)
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    url = f"{ROAM_API_BASE}/{graph_name}/{endpoint}"
    print(f"Full URL: {url}", file=sys.stderr)

    async with httpx.AsyncClient() as client:
        try:
            if method.lower() == "get":
                response = await client.get(url, headers=headers)
            else:
                response = await client.post(url, headers=headers, json=json_data)
            
            print(f"Response status: {response.status_code}", file=sys.stderr)
            if response.status_code == 308:
                print(f"Received redirect response. Headers: {response.headers}", file=sys.stderr)
                
            if response.status_code != 200:
                raise Exception(
                    f"API request failed with status code {response.status_code}: {response.text}"
                )
            
            return response.json()
        except Exception as e:
            print(f"Error in make_roam_request: {str(e)}", file=sys.stderr)
            raise


def extract_youtube_video_id(url: str) -> Optional[str]:
    """Extract the video ID from a YouTube URL."""
    found = re.search(r"(?:youtu\.be\/|watch\?v=)([\w-]+)", url)
    if found:
        return found.group(1)
    return None


def process_results(raw_results):
    """Process raw search results to extract content, remove duplicates, and limit word count."""
    unique_strings = set()
    processed_results = []
    word_count = 0
    max_word_count = 3000  # Maximum word count limit

    for block_list in raw_results:
        for block in block_list:
            block_string = block.get(':block/string')
            if block_string and block_string not in unique_strings:
                # Count the number of words in the block string
                block_word_count = len(block_string.split())

                # Check if adding this block string exceeds the word limit
                if word_count + block_word_count <= max_word_count:
                    unique_strings.add(block_string)
                    processed_results.append(block_string)
                    word_count += block_word_count
                else:
                    # Stop processing if the word limit is reached
                    return processed_results

    return processed_results


async def find_block_uid(api_token, graph_name, block_content):
    """Search for a block by its content to find its UID."""
    search_query = f'''[:find (pull ?e [:block/uid])
                      :where [?e :block/string "{block_content}"]]'''

    search_response = await make_roam_request("post", "q", api_token,
                                              graph_name,
                                              {"query": search_query})

    if search_response.get('result'):
        block_uid = search_response['result'][0][0][':block/uid']
        return block_uid
    else:
        raise Exception("Failed to find the newly created block UID.")


async def create_block(api_token, graph_name, parent_uid, block_content,
                       block_order):
    """Create a block and handle child blocks by finding the new block's UID."""
    block_data = {
        "action": "create-block",
        "location": {
            "parent-uid": parent_uid,
            "order": block_order
        },
        "block": {
            "string": block_content['text']
        }
    }

    block_resp = await make_roam_request("post", "write", api_token,
                                         graph_name, block_data)

    # If the block has children, recursively handle them
    if 'children' in block_content:
        new_parent_uid = await find_block_uid(api_token, graph_name,
                                              block_content['text'])
        for order, child in enumerate(block_content['children']):
            await create_block(api_token, graph_name, new_parent_uid, child,
                               order)

    return block_resp


def get_roam_credentials():
    """Get Roam API token and graph name from environment variables."""
    # Print all environment variables for debugging
    print("Environment variables:", file=sys.stderr)
    for key, value in os.environ.items():
        # Print key and first few characters of value for security
        value_preview = value[:5] + "..." if len(value) > 5 else value
        print(f"  {key}: {value_preview}", file=sys.stderr)
    
    api_token = os.environ.get("ROAM_API_TOKEN")
    graph_name = os.environ.get("ROAM_GRAPH_NAME")
    
    if not api_token:
        print("Error: ROAM_API_TOKEN environment variable is not set", file=sys.stderr)
    else:
        print(f"Found ROAM_API_TOKEN starting with: {api_token[:5]}...", file=sys.stderr)
        
    if not graph_name:
        print("Error: ROAM_GRAPH_NAME environment variable is not set", file=sys.stderr)
    else:
        print(f"Found ROAM_GRAPH_NAME: {graph_name}", file=sys.stderr)
        
    return api_token, graph_name


@mcp.tool()
async def search_roam(search_terms: List[str]) -> str:
    """Search Roam database for content containing the specified terms.

    Args:
        search_terms: List of keywords to search for
    """
    api_token, graph_name = get_roam_credentials()
    if not api_token or not graph_name:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    all_results = []

    for keyword in search_terms:
        query = f'''[:find (pull ?b [*])
                     :where [?b :block/string ?s]
                            [(clojure.string/includes? ?s "{keyword}")]]'''

        data = {"query": query.replace("\n", " ")}

        response = await make_roam_request("post", "q", api_token, graph_name,
                                           data)
        all_results.extend(response.get('result', []))

    # Process results to extract content, remove duplicates, and limit word count
    processed_results = process_results(all_results)

    return "\n\n".join(processed_results)


@mcp.tool()
async def create_page(page_title: str, content: List[Dict]) -> str:
    """Create a new page in Roam Research and link it in daily notes.

    Args:
        page_title: Title for the new page
        content: List of content blocks to add to the page
    """
    api_token, graph_name = get_roam_credentials()
    if not api_token or not graph_name:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    # Check if page exists
    find_page_query = f'''[:find ?uid
                         :where [?e :node/title "{page_title}"]
                                [?e :block/uid ?uid]]'''

    find_page_resp = await make_roam_request("post", "q", api_token,
                                             graph_name,
                                             {"query": find_page_query})
    page_exists = find_page_resp and find_page_resp.get('result')
    page_uid = find_page_resp.get('result',
                                  [[None]])[0][0] if page_exists else None

    # Create page if it doesn't exist
    if not page_exists:
        create_page_data = {
            "action": "create-page",
            "page": {
                "title": page_title
            }
        }
        create_page_resp = await make_roam_request("post", "write", api_token,
                                                   graph_name,
                                                   create_page_data)

        if "page" not in create_page_resp or "uid" not in create_page_resp.get(
                "page", {}):
            raise Exception("Failed to create new page")

        page_uid = create_page_resp["page"]["uid"]

    # Add content to the page
    for block_order, block_content in enumerate(content):
        await create_block(api_token, graph_name, page_uid, block_content,
                           block_order)

    # Link in today's daily notes
    try:
        today_date = datetime.now().strftime("%B %-dth, %Y")
        daily_notes_query = f'''[:find ?uid
                                 :where [?e :node/title "{today_date}"]
                                        [?e :block/uid ?uid]]'''

        daily_notes_resp = await make_roam_request(
            "post", "q", api_token, graph_name, {"query": daily_notes_query})

        if daily_notes_resp and daily_notes_resp.get('result'):
            daily_notes_uid = daily_notes_resp['result'][0][0]
            link_block_data = {
                "action": "create-block",
                "location": {
                    "parent-uid": daily_notes_uid,
                    "order": 0
                },
                "block": {
                    "string": f"[[{page_title}]]"
                }
            }
            await make_roam_request("post", "write", api_token, graph_name,
                                    link_block_data)
    except Exception as e:
        # Continue even if linking fails
        pass

    # Return link to the page
    roam_page_link = f"https://roamresearch.com/#/app/{graph_name}/page/{page_uid}"
    return f"Content added to page and linked in Daily Notes: {roam_page_link}"


@mcp.tool()
async def get_youtube_transcript(url: str) -> str:
    """Fetch and return the transcript of a YouTube video.

    Args:
        url: URL of the YouTube video
    """
    from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled

    video_id = extract_youtube_video_id(url)
    if not video_id:
        return "Invalid YouTube URL. Unable to extract video ID."

    try:
        # Define the prioritized list of language codes
        languages = [
            'en', 'en-US', 'en-GB', 'de', 'es', 'hi', 'zh', 'ar', 'bn', 'pt',
            'ru', 'ja', 'pa'
        ]

        # Attempt to retrieve the available transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Try to find a transcript in the prioritized languages
        for language in languages:
            try:
                transcript = transcript_list.find_transcript([language])
                # Check if the transcript is manually created or generated, prefer manually created
                if transcript.is_generated:
                    continue
                text = " ".join([line["text"] for line in transcript.fetch()])
                return text
            except Exception:
                continue

        # If no suitable transcript is found in the specified languages, try to fetch a generated transcript
        try:
            generated_transcript = transcript_list.find_generated_transcript(
                languages)
            text = " ".join(
                [line["text"] for line in generated_transcript.fetch()])
            return text
        except Exception:
            return "No suitable transcript found for this video."

    except TranscriptsDisabled:
        return "Transcripts are disabled for this video."
    except Exception as e:
        return f"An error occurred while fetching the transcript: {str(e)}"


@mcp.tool()
async def get_roam_graph_info() -> str:
    """
    Get information about a Roam Research graph.
    """
    # Get API token and graph name from environment variables
    api_token, graph_name = get_roam_credentials()
    if not api_token or not graph_name:
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        # Get basic graph information
        graph_info_query = '''[:find (pull ?g [*])
                               :where [?g :graph/slug]]'''

        graph_info = await make_roam_request("post", "q", api_token,
                                             graph_name,
                                             {"query": graph_info_query})

        # Get page count
        page_count_query = '''[:find (count ?p)
                               :where [?p :node/title]]'''

        page_count = await make_roam_request("post", "q", api_token,
                                             graph_name,
                                             {"query": page_count_query})

        # Format the output
        formatted_info = f"""
Graph Name: {graph_name}
Pages: {page_count['result'][0][0] if page_count.get('result') else 'Unknown'}
API Access: Enabled
        """

        return formatted_info
    except Exception as e:
        return f"Error retrieving graph information: {str(e)}"


@mcp.prompt()
async def summarize_page(page_title: str) -> dict:
    """
    Create a prompt to summarize a page in Roam Research.

    Args:
        page_title: Title of the page to summarize
    """
    # Get API token and graph name from environment variables
    api_token, graph_name = get_roam_credentials()
    if not api_token or not graph_name:
        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type": "text",
                    "text": "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
                }
            }]
        }
    
    # Query to get the page content
    query = f'''[:find (pull ?b [:block/string])
                 :where [?p :node/title "{page_title}"]
                        [?b :block/page ?p]]'''

    try:
        response = await make_roam_request("post", "q", api_token, graph_name,
                                           {"query": query})

        page_blocks = [
            block[0].get(':block/string', '')
            for block in response.get('result', [])
        ]
        page_content = "\n".join(page_blocks)

        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type":
                    "text",
                    "text":
                    f"Please provide a concise summary of the following page content from my Roam Research database:\n\n{page_content}"
                }
            }]
        }
    except Exception as e:
        return {
            "messages": [{
                "role": "user",
                "content": {
                    "type":
                    "text",
                    "text":
                    f"I wanted to summarize my Roam page titled '{page_title}', but there was an error retrieving the content: {str(e)}. Can you help me troubleshoot this issue with my Roam Research integration?"
                }
            }]
        }


def run_server(transport="stdio", port=None):
    """Run the MCP server with the specified transport."""
    # Print all environment variables at startup for debugging
    print("Server starting...", file=sys.stderr)
    print("Environment variables:", file=sys.stderr)
    for key, value in os.environ.items():
        # Print key and first few characters of value for security
        value_preview = value[:5] + "..." if len(value) > 5 else value
        print(f"  {key}: {value_preview}", file=sys.stderr)
    
    # FastMCP.run() doesn't accept a port parameter, so we ignore it
    mcp.run(transport=transport)